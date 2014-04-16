#!/usr/bin/env python

# This file is part of NNTSC
#
# Copyright (C) 2013 The University of Waikato, Hamilton, New Zealand
# Authors: Shane Alcock
#          Brendon Jones
#          Nathan Overall
#
# All rights reserved.
#
# This code has been developed by the WAND Network Research Group at the
# University of Waikato. For more information, please see
# http://www.wand.net.nz/
#
# This source code is proprietary to the University of Waikato and may not be
# redistributed, published or disclosed without prior permission from the
# University of Waikato and the WAND Network Research Group.
#
# Please report any bugs, questions or comments to contact@wand.net.nz
#
# $Id$


import sys, socket, time, struct, getopt, pickle
from socket import *
from multiprocessing import Pipe, Queue
import Queue as StdQueue
import threading, select

from libnntsc.dbselect import DBSelector
from libnntsc.configurator import *
from libnntscclient.protocol import *
from libnntscclient.logger import *
from libnntsc.pikaqueue import initExportConsumer, PIKA_CONSUMER_HALT, \
        PIKA_CONSUMER_RETRY
from libnntsc.dberrorcodes import *

# There are 4 classes defined in this file that form a hierarchy for
# exporting live and/or historical data to clients.
#
# The hierarchy is as follows:
#
# NNTSC has 1 instance of the NNTSCExporter class. It listens for new
# client connections and ensures that any new live data is passed on
# to any clients that have subscribed to it. The NNTSC dataparsers
# register themselves as sources to the NNTSCExporter on start-up and
# send all new data to the exporter via a rabbit queue. The queue is
# disk-backed and persistent so there is less urgency than there used to
# be about reading data from this queue.  
#
# When it first starts running, the NNTSCExporter creates a thread for
# a NNTSCListener to run in. This thread is solely dedicated to listening
# for client connections. The reason this is a separate thread is to allow
# us to not have to deal with switching between reading from the queue and
# accepting connections within the NNTSCExporter, as it wasn't entirely
# clear how to make asynchronous consumers work together with other
# asynchronous events (e.g. select).
#
# When a new client connects, a new instance of NNTSCClient is created
# which runs in a separate thread. This thread deals with reading requests
# for data from the client and sending
# the responses back to the client. It also handles forwarding any live
# data that the NNTSCExporter passes on to it -- this particular task
# has higher priority than the others. 
#
# Whenever a NNTSCClient receives a request for data that requires a
# database query, e.g. historical data or a list of streams, the job is
# farmed out to an instance of the DBWorker class. Each NNTSCClient will
# own a number of DBWorker threads (equal to MAX_WORKERS) which can
# query the database without affecting processing of live data or new
# client requests. When the query completes, the results are packed into
# NNTSC response messages and returned back to the NNTSCClient instance via yet 
# another queue for subsequent transmission to the client that requested them. 
# Before doing so, the
# DBWorker thread will also run over the results, transforming them into
# a nice dictionary mapping column names to values and estimating the
# measurement frequency (which is required by most client applications).
#
# In summary:
#   1 NNTSCExporter
#   1 NNTSCListener
#   1 NNTSCClient thread per connected client
#   MAX_WORKERS DBWorker threads per NNTSCClient

MAX_HISTORY_QUERY = (24 * 60 * 60 * 7)
MAX_WORKERS = 2

DB_WORKER_MAX_RETRIES = 3

DBWORKER_SUCCESS = 1
DBWORKER_RETRY = 0
DBWORKER_ERROR = -1
DBWORKER_BADJOB = -2
DBWORKER_FULLQUEUE = -3
DBWORKER_HALT = -4

class DBWorker(threading.Thread):
    def __init__(self, parent, queue, dbconf, threadid, timeout):
        threading.Thread.__init__(self)
        self.dbconf = dbconf
        self.parent = parent
        self.queue = queue
        self.threadid = threadid
        self.timeout = timeout
        self.db = None

    def process_job(self, job):
        jobtype = job[0]
        jobdata = job[1]

        if jobtype == -1:
            return DBWORKER_HALT
   
        self.retries = 0
    
        if jobtype == NNTSC_REQUEST:
            return self.process_request(jobdata)

        if jobtype == NNTSC_AGGREGATE:
            return self.aggregate(jobdata)

        if jobtype == NNTSC_PERCENTILE:
            # XXX can we inform the client they asked for something we
            # don't support?
            log("Client requested percentile data, but we don't support that anymore")
            return DBWORKER_BADJOB

        if jobtype == NNTSC_SUBSCRIBE:
            return self.subscribe(jobdata)

        return DBWORKER_BADJOB

    def _merge_aggregators(self, columns, func):
        # Combine the aggcols and aggfunc variables into a nice single
        # list to match the format now expected by the dbselect functions
        aggs = []

        if type(func) is str:
            for c in columns:
                aggs.append((c,func))

        elif len(func) == 1:
            for c in columns:
                aggs.append((c, func[0]))

        elif len(func) == len(columns):
            for i in range(0, len(columns)):
                aggs.append((columns[i], func[i]))


        return aggs

    def aggregate(self, aggmsg):
        tup = pickle.loads(aggmsg)
        name, start, end, labels, aggcols, groupcols, binsize, aggfunc = tup
        now = int(time.time())

        if end == 0:
            end = None

        if start == None or start >= now:
            # No historical data, send empty history for all streams
            for lab, streams in labels.items():
                err = self._enqueue_history(name, lab, [], False, 0, now)
                if err != DBWORKER_SUCCESS:
                    return err 
            return DBWORKER_SUCCESS

        if end == None:
            stoppoint = int(time.time())
        else:
            stoppoint = end

        aggs = self._merge_aggregators(aggcols, aggfunc)

        while start < stoppoint:
            queryend = start + MAX_HISTORY_QUERY

            if queryend >= stoppoint:
                queryend = stoppoint
                more = False
            else:
                more = True
                # Make sure our queries align nicely with the binsize,
                # otherwise we'll end up with duplicate results for the
                # bins that span the query boundary
                if queryend % binsize < binsize - 1:
                    queryend = (int(queryend / binsize) * binsize) - 1

            generator = self.db.select_aggregated_data(name, labels, aggs,
                    start, queryend, groupcols, binsize)

            error = self._query_history(generator, name, labels, more, start,
                    queryend)

            if error == DBWORKER_RETRY:
                continue
            if error != DBWORKER_SUCCESS:
                return error

            start = queryend + 1

            # If we were asked for data up until "now", make sure we account
            # for the time taken to make earlier queries otherwise we'll
            # miss any new data inserted while we were querying previous
            # weeks of data
            if end == None:
                stoppoint = int(time.time())

        error = self.db.release_data()
        if error != DB_NO_ERROR:
            return DBWORKER_ERROR

        return DBWORKER_SUCCESS

    def subscribe(self, submsg):
        name, start, end, cols, labels, aggs = pickle.loads(submsg)
        now = int(time.time())

        origstart = start
        if start == 0 or start == None:
            start = now
        if end == 0:
            end = None

        if start >= now:
            # No historical data, send empty history for all streams
            for lab, streams in labels.items():
                err = self._enqueue_history(name, lab, [], False, 0, start)
                if err != DBWORKER_SUCCESS:
                    return err

                err = self._subscribe_streams(streams, start, end, cols, name, lab)
                if err != DBWORKER_SUCCESS:
                    return err

            return 0
        
        if end == None:
            stoppoint = int(time.time())
        else:
            stoppoint = end

        if aggs != []:
            aggcols = self._merge_aggregators(cols, aggs)
            

        # Register our interest in these streams before we start querying, so
        # the client can stockpile any live data that arrives while we're
        # busy querying the db.
        for lab, streams in labels.items():
            err = self._subscribe_streams(streams, start, end, cols, name, lab)
            if err != DBWORKER_SUCCESS:
                return err

        while start <= stoppoint:
            queryend = start + MAX_HISTORY_QUERY
    
            # If we were asked for data up until "now", make sure we account
            # for the time taken to make earlier queries otherwise we'll
            # miss any new data inserted while we were querying previous
            # weeks of data
            #
            # We're still going to miss anything that arrives between here and
            # whenever we manage to complete the last query but we want
            # our data to arrive in order (i.e. history before any live), so
            # that's kinda tricky. Usually, we're only going to miss one 
            # measurement though.
            #
            # XXX Can we subscribe before doing the last query and then
            # funnel any live data into temporary storage until the query
            # completes. Once we're caught up, throw all that saved live data
            # onto the queue. 
            #if end == None:
            #    stoppoint = int(time.time())

            if queryend >= stoppoint:
                queryend = stoppoint
                more = False
            else:
                more = True

            # Only aggregate the streams for each label if explicitly requested,
            # otherwise fetch full historical data
            if aggs != []:
                generator = self.db.select_aggregated_data(name, labels,
                        aggcols, start, queryend, [], 1)
            else:
                generator = self.db.select_data(name, labels, cols, start,
                        queryend)

            error = self._query_history(generator, name, labels, more, start,
                    queryend)

            if error == DBWORKER_RETRY:
                continue
            if error != DBWORKER_SUCCESS:
                return error

            start = queryend + 1

        error = self.db.release_data()
        if error != DB_NO_ERROR:
            return DBWORKER_ERROR

        #log("Subscribe job completed successfully (%s)\n" % (self.threadid))
        return DBWORKER_SUCCESS

    def _cancel_history(self, name, labels, start, end, more):
        # If the query was cancelled, let the client know that
        # the absence of data for this time range is due to a 
        # database query timeout rather than a lack of data
        err = self._enqueue_cancel(NNTSC_HISTORY, (name, labels, start, end, more))
        if err != DBWORKER_SUCCESS:
            return err

        if not more:
            for lab in labels:
                try:
                    self.queue.put((NNTSC_HISTORY_DONE, lab, 0), False)
                except StdQueue.Full:
                    log("Unable to push history onto full worker queue")
                    return DBWORKER_FULLQUEUE

        return DBWORKER_SUCCESS

    def _query_history(self, rowgen, name, labels, more, start, end):

        currlabel = -1
        historysize = 0

        observed = set([])

        # Get any historical data that we've been asked for
        for row, tscol, binsize, exception in rowgen:

            if exception is not None:
                log(exception)
                if exception.code == DB_QUERY_TIMEOUT:
                    return self._cancel_history(name, labels, start, end, more)
                elif exception.code == DB_OPERATIONAL_ERROR:
                    if self._reconnect_database() == -1:
                        return DBWORKER_ERROR 
                    return DBWORKER_RETRY
                else:
                    return DBWORKER_ERROR

            # Limit the amount of history we export at any given time
            # to prevent us from using too much memory during processing
            if row['label'] != currlabel or historysize > 10000:
                if currlabel != -1:

                    # We've reached the history limit, so make sure we
                    # avoid duplicate subscriptions and set the 'more'
                    # flag correctly
                    if row['label'] == currlabel:
                        thismore = True
                    else:
                        thismore = more
                           
                    # Export the history to the pipe
                    freq = self._calc_frequency(freqstats, binsize)
                    lastts = freqstats['lastts']

                    assert(currlabel in labels)
                    err = self._enqueue_history(name, currlabel, history, thismore, freq, lastts)
                    if err != DBWORKER_SUCCESS:
                        return err
                    
                # Reset all our counters etc.
                freqstats = {'lastts': 0, 'lastbin':0, 'perfectbins':0,
                            'totaldiffs':0, 'tsdiffs':{} }
                currlabel = row['label']
                assert(currlabel in labels)

                history = []
                historysize = 0
                observed.add(currlabel)

            # Extract info needed for measurement frequency calculations
            if freqstats['lastts'] == 0:
                freqstats['lastts'] = row['timestamp']
                freqstats['lastbin'] = row[tscol]
            elif freqstats['lastts'] != row['timestamp']:
                tsdiff = row['timestamp'] - freqstats['lastts']
                bindiff = row[tscol] - freqstats['lastbin']

                if bindiff == binsize:
                    freqstats['perfectbins'] += 1

                if tsdiff in freqstats['tsdiffs']:
                    freqstats['tsdiffs'][tsdiff] += 1
                else:
                    freqstats['tsdiffs'][tsdiff] = 1

                freqstats['totaldiffs'] += 1
                freqstats['lastts'] = row['timestamp']
                freqstats['lastbin'] = row[tscol]

            # Convert the row into a nice dictionary and add it to the
            # history to be exported
            datadict = {}
            for k, v in row.items():
                datadict[k] = v
            history.append(datadict)
            historysize += 1

        if historysize != 0:
            # Make sure we write out the last stream
            freq = self._calc_frequency(freqstats, binsize)
            lastts = freqstats['lastts']
            err = self._enqueue_history(name, currlabel, history, more, freq, lastts)
            if err != DBWORKER_SUCCESS:
                return err


        # Also remember to export empty history for any streams that had
        # no data in the request period
        # XXX convert to string to work temporarily with old style stream_ids
        allstreams = set(labels.keys())

        missing = allstreams - observed
        for m in missing:
            assert (m in labels)
            err = self._enqueue_history(name, m, [], more, 0, 0)
            if err != DBWORKER_SUCCESS:
                return err

        return DBWORKER_SUCCESS


    def _subscribe_streams(self, streams, start, subend, cols, name, label):
        try:
            self.queue.put((NNTSC_SUBSCRIBE, (streams, start, subend, cols, name, label)), False)
        except StdQueue.Full:
            log("DBWorker tried to push subscribe but result queue was full!")
            return DBWORKER_FULLQUEUE
        return DBWORKER_SUCCESS

    def _enqueue_cancel(self, reqtype, data):
        contents = pickle.dumps((reqtype, data))
        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_QUERY_CANCELLED, len(contents))

        try:
            self.queue.put((NNTSC_QUERY_CANCELLED, header + contents), False)
        except error, msg:
            log("Unable to push query cancelled message onto full worker queue")
            return DBWORKER_FULLQUEUE
        return DBWORKER_SUCCESS

    def _enqueue_history(self, name, label, history, more, freq, lastts):

        contents = pickle.dumps((name, label, history, more, freq))
        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_HISTORY, len(contents))

        try:
            self.queue.put((NNTSC_HISTORY, header + contents), False)
        except StdQueue.Full:
            log("Unable to push history onto full worker queue")
            return DBWORKER_FULLQUEUE

        if not more:
            try:
                self.queue.put((NNTSC_HISTORY_DONE, label, lastts), False)
            except StdQueue.Full:
                log("Unable to push history onto full worker queue")
                return DBWORKER_FULLQUEUE

        return DBWORKER_SUCCESS

    def _reconnect_database(self):
        if self.retries >= DB_WORKER_MAX_RETRIES:
            return DBWORKER_ERROR
        self.retries += 1
        log("Worker thread %s reconnecting to NNTSC database: attempt %d" % \
                (self.threadid, self.retries))

        self._connect_database()
        return DBWORKER_SUCCESS

    def _request_collections(self):
        # Requesting the collection list
         
        try:
            cols = self.db.list_collections()
        except DBQueueException as e:
            if e.code == DB_QUERY_TIMEOUT:
                log("Query timed out while fetching collections")
                err = self._enqueue_cancel(NNTSC_COLLECTIONS, None)    
                if err != DBWORKER_SUCCESS:
                    return err
                return DBWORKER_SUCCESS
            else:
                log("Exception while fetching collections: %s" % (e))
                return DBWORKER_ERROR

        shrink = []
        for c in cols:
            shrink.append({"id":c['id'], "module":c['module'], "modsubtype":c['modsubtype']})

        col_pickle = pickle.dumps(shrink)
        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_COLLECTIONS, len(col_pickle))

        try:
            self.queue.put((NNTSC_COLLECTIONS, header + col_pickle), False)
        except StdQueue.Full:
            log("Failed to write collections to full DBWorker result queue");
            return DBWORKER_FULLQUEUE

        return DBWORKER_SUCCESS

    def _request_schemas(self, col_id):

        try:
            stream_schema, data_schema = self.db.get_collection_schema(col_id)
        except DBQueryException as e:
            if e.code == DB_QUERY_TIMEOUT:
                log("Query timed out while fetching schemas for collection %d" % (col_id))

                err = self._enqueue_cancel(NNTSC_SCHEMAS, col_id)
                if err != DBWORKER_SUCCESS:
                    log("Failed to enqueue CANCELLED message")
                    return err
                return DB_WORKER_SUCCESS
            else:
                log("Exception while fetching schemas: %s" % (e))
                return DBWORKER_ERROR

        result = pickle.dumps((col_id, stream_schema, data_schema))
        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_SCHEMAS, len(result))

        try:
            self.queue.put((NNTSC_SCHEMAS, header + result), False)
        except StdQueue.Full:
            log("Failed to write schemas to full DBWorker result queue");
            return DBWORKER_FULLQUEUE
        return DBWORKER_SUCCESS

    def _request_streams(self, col, bound, request):
        
        if request == NNTSC_STREAMS:
            try:
                streams = self.db.select_streams_by_collection(col, bound)
            except DBQueryException as e:
                if e.code == DB_QUERY_TIMEOUT:
                    log("Query timed out while fetching streams for collection %s" % (col))
                    err = self._enqueue_cancel(NNTSC_STREAMS, (col, bound))
                    if err != DBWORKER_SUCCESS:
                        log("Failed to enqueue CANCELLED message")
                        return err
                    return DBWORKER_SUCCESS
                else:
                    log("Exception while fetching streams: %s" % (e))
                    return DBWORKER_ERROR
                
        elif request == NNTSC_ACTIVE_STREAMS:
            try:
                streams = self.db.select_active_streams_by_collection(col, bound)
            except DBQueryException as e:
                if e.code == DB_QUERY_TIMEOUT:
                    log("Query timed out while fetching active streams for collection %s" % (col))
                    err = self._enqueue_cancel(NNTSC_ACTIVE_STREAMS, (col, bound))
                    if err != DBWORKER_SUCCESS:
                        log("Failed to enqueue CANCELLED message")
                        return err
                    return DBWORKER_SUCCESS
                else:
                    log("Exception while fetching active streams: %s" % (e))
                    return DBWORKER_ERROR
        else:
            log("Got into request streams with bad request: %d" % request)
            return DBWORKER_BADJOB
       
        try:
            self.queue.put((NNTSC_REGISTER_COLLECTION, col), False)
        except StdQueue.Full:
            log("Failed to register collection %d due to full DBWorker result queue" % (col));
            return DBWORKER_FULLQUEUE
        
        if len(streams) == 0:
            return self._enqueue_streams(request, col, False, [])

        i = 0
        while (i < len(streams)):
            start = i
            if len(streams) <= i + 1000:
                end = len(streams)
                more = False
            else:
                end = i + 1000
                more = True

            err = self._enqueue_streams(request, col, more, streams[start:end])
            if err != DBWORKER_SUCCESS:
                log("Failed on streams %d:%d (out of %d))" % (start, end, len(streams)))
                return err

            i = end
        return DBWORKER_SUCCESS

    def _enqueue_streams(self, req, col, more, streams):
        stream_data = pickle.dumps((col, more, streams))
        header = struct.pack(nntsc_hdr_fmt, 1, req, len(stream_data))

        try:
            self.queue.put((req, header + stream_data), False)
        except StdQueue.Full:
            log("Failed to write streams to full DBWorker result queue");
            return DBWORKER_FULLQUEUE
        return DBWORKER_SUCCESS
       

    # Processes the job for a basic NNTSC request, i.e. asking for the
    # collections, schemas or streams rather than querying for time
    # series data
    def process_request(self, reqmsg):
        req_hdr = struct.unpack(nntsc_req_fmt,
                reqmsg[0:struct.calcsize(nntsc_req_fmt)])


        if req_hdr[0] == NNTSC_REQ_COLLECTION:
            return self._request_collections()

        if req_hdr[0] == NNTSC_REQ_SCHEMA:
            return self._request_schemas(req_hdr[1])
        
        if req_hdr[0] == NNTSC_REQ_STREAMS:
            return self._request_streams(req_hdr[1], req_hdr[2], NNTSC_STREAMS)

        if req_hdr[0] == NNTSC_REQ_ACTIVE_STREAMS:
            return self._request_streams(req_hdr[1], req_hdr[2], NNTSC_ACTIVE_STREAMS)

        return 0

    def _connect_database(self):
        self.db = DBSelector(self.threadid, self.dbconf["name"], 
                self.dbconf["user"],
                self.dbconf["pass"], self.dbconf["host"], self.timeout)
        self.db.connect_db(30)

    def run(self):
        running = 1

        while running:
            # Wait for a job to become available
            try:
                job = self.parent.jobs.get(True)
            except StdQueue.Empty:
                continue
            except EOFError:
                break

            # Don't have a db connection open for the lifetime of
            # this thread if we aren't using it otherwise we run the
            # risk of inactive threads preventing us from contacting
            # the database
            self._connect_database()
            err = self.process_job(job)
            if err == DBWORKER_HALT:
                break
            if err != DBWORKER_SUCCESS:
                log("Failed to process job, error code %d -- dropping client" % (err))
                break
            self.db.disconnect()
            self.db = None

        # Thread is over, tidy up
        if self.db is not None:
            self.db.disconnect()
        self.parent.disconnect()

    def _calc_frequency(self, freqdata, binsize):

        # Long and complicated explanation follows....
        #
        # We need to know the 'binsize' so that we can determine whether
        # there are missing measurements in the returned result. This is
        # useful for leaving gaps in our graphs where data was missing.
        #
        # The database will give us results that are binned according to the
        # requested binsize, but this binsize may be smaller than the
        # measurement frequency. If it is, we can't use the requested binsize
        # to determine whether a measurement is missing because there will be
        # empty bins simply because no measurement fell within that time
        # period.
        #
        # Instead, we actually want to know the measurement frequency or
        # the binsize, whichever is bigger. The problem is that we don't have
        # any obvious way of knowing the measurement frequency, so we have to
        # infer it.
        #
        # Each row in the result object corresponds to a bin. For
        # non-aggregated data, the bin will always only cover one data
        # measurement.
        #
        # There are two timestamps associated with each result row:
        #
        #   'binstart' is the timestamp where a bin begins and is calculated
        #   based on the requested bin size. For non-aggregated data, this is
        #   the same as the timestamp of the data point.
        #
        #   'timestamp' is the timestamp of the *last* measurement included in
        #   the bin. This is the timestamp we use for plotting graphs.
        #
        #
        # There are two main cases to consider:
        #   1. The requested binsize is greater than or equal to the
        #      measurement frequency. In this case, use the requested binsize.
        #   2. The requested binsize is smaller than the measurement frequency.
        #      In this case, we need to use the measurement frequency.
        #
        # In case 1, the vast majority of bins are going to be separated by
        # the requested binsize. So we can detect this case by looking at the
        # number of occasions the time difference between bins (using
        # 'binstart' matches the binsize that we requested.
        #
        # Case 2 is trickier. Once we rule out case 1, we need to guess what
        # the measurement frequency is. Fortunately, we know that each bin
        # can only contain 1 measurement at most, so we can use the
        # 'timestamp' field from consecutive bins to infer the frequency.
        # We collect these time differences and use the mode of these values
        # as our measurement frequency. This will work even if the requested
        # binsize is not a factor of the measurement frequency.

        # XXX Potential pitfalls
        # * What if there are a lot of non-consecutive missing measurements?
        # * What if the test changes its measurement frequency?

        # No measurements were observed, use the binsize if sane
        if freqdata['totaldiffs'] == 0:
            if binsize != 0:
                return 300
            else:
                return binsize

        # 90% of the timestamp differences match the binsize, so just use it
        if freqdata['perfectbins'] / float(freqdata['totaldiffs']) > 0.9:
            return binsize

        # Set some sensible defaults -- don't set the default binsize too small
        if binsize < 300:
            freq = 300
        else:
            freq = binsize

        # Find a suitable mode amongst the timestamp differences. Make sure
        # the mode is reasonably strong -- shouldn't be much variation in
        # timestamp differences unless your measurements are patchy.
        for td, count in freqdata['tsdiffs'].items():
            if count >= 0.5 * freqdata['totaldiffs']:
                freq = td
                break

            # If there is no strong mode, go with the smallest somewhat
            # prominent value
            if count >= 0.2 * freqdata['totaldiffs'] and td < freq:
                freq = td

        return freq

class NNTSCClient(threading.Thread):
    def __init__(self, sock, parent, queue, dbconf, dbtimeout):
        threading.Thread.__init__(self)
        assert(dbconf)
        self.sock = sock
        self.parent = parent
        self.livequeue = queue
        self.workdone = Queue(20000)
        self.recvbuf = ""
        self.jobs = Queue(100000)
        self.releasedlive = Queue(100000)
        self.outstanding = ""

        self.running = 0
        self.waitstreams = {}
        self.waitlabels = {}
        self.savedlive = {} 
        
        self.workers = []
        # Create some worker threads for handling the database queries
        for i in range(0, MAX_WORKERS):
            threadid = "client%d_thread%d" % (self.sock.fileno(), i)

            worker = DBWorker(self, self.workdone, dbconf, threadid, dbtimeout)
            worker.daemon = True
            worker.start()

            self.workers.append(worker)

    def subscribe_stream(self, submsg):
        streams, start, end, cols, name, label = submsg

        if label not in self.waitlabels:
            self.waitlabels[label] = streams
        else:
            return
        
        for s in streams:
            self.waitstreams[s] = label

        assert(label not in self.savedlive)
        self.savedlive[label] = []

        self.parent.register_stream(streams, self.sock, cols, start, end, name, label)

    def finish_subscribe(self, label, lasthist):
        # History has all been sent for this label, so we can now release
        # any live data we were storing for those streams

        if label not in self.waitlabels:
            return 0

        assert(label in self.waitlabels)
        streams = self.waitlabels[label]
        data = self.savedlive[label]

        if len(data) != 0:
            ts = 0
            
            for d in data:
                contents = pickle.loads(d[1])

                if ts == 0:
                    ts = d[2]

                if d[2] != ts:
                    # Chuck a PUSH on the queue
                    pushdata = pickle.dumps((contents[0], ts))
                    header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_PUSH, 
                            len(pushdata))
                    
                    try:
                        self.releasedlive.put((header, pushdata), True, 10)
                    except StdQueue.Full:
                        log("Could not release stored live data, queue full")
                        log("Dropping client")
                        return -1
                    
                    ts = d[2]

                if d[2] <= lasthist:
                    continue

                try:
                    self.releasedlive.put(d, True, 10)
                except StdQueue.Full:
                    log("Could not release stored live data, queue full")
                    log("Dropping client")
                    return -1

            # One final push
            pushdata = pickle.dumps((contents[0], ts))
            header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_PUSH, 
                    len(pushdata))
            try:
                self.releasedlive.put((header, pushdata), True, 10)
            except StdQueue.Full:
                log("Could not release stored live data, queue full")
                log("Dropping client")
                return -1

        del(self.savedlive[label])
        
        for s in streams:
            if s in self.waitstreams:
                del(self.waitstreams[s])

        del self.waitlabels[label]
           
        return 0   
         
    def client_message(self, msg):
        error = 0
        header = struct.unpack(nntsc_hdr_fmt, msg[0:struct.calcsize(nntsc_hdr_fmt)])
        total_len = header[2] + struct.calcsize(nntsc_hdr_fmt)
        body = msg[struct.calcsize(nntsc_hdr_fmt):total_len]

        if len(msg) < total_len:
            return msg, 1, error

        # Put the job on our joblist for one of the worker threads to handle
        try:
            self.jobs.put((header[1], body), False)
        except StdQueue.Full:
            log("Too many jobs queued by a client, dropping client")
            return msg, 0, 1

        return msg[total_len:], 0, error

    def receive_client(self):

        buf = self.recvbuf
        fd = self.sock.fileno()

        try:
            received = self.sock.recv(4096)
        except Exception, msg:
            log("Error receiving data from client %d: %s" % (fd, msg))
            return 0

        if len(received) == 0:
            # Client has disconnected
            return 0

        buf = buf + received
        error = 0

        while len(buf) > struct.calcsize(nntsc_hdr_fmt):
            buf, halt, error = self.client_message(buf)

            if halt or error == 1:
                break

        if error != 1:
            self.recvbuf = buf
        if error == 1:
            return 0

    def receive_live(self):
        
        while 1:
            try:
                obj = self.livequeue.get(False)
            except StdQueue.Empty:
                return 0
            
            sendobj = obj[0] + obj[1]
            contents = pickle.loads(obj[1])
            streamid = contents[1]

            if streamid in self.waitstreams:
                # Still waiting for history to come back for this stream.
                # Save this live data so we can send it after the history is
                # complete.
                lab = self.waitstreams[streamid]
                self.savedlive[lab].append(obj)
                #log("Saving data for stream %d -- %s" % (streamid, lab))
                continue

            # Just reflect the objects directly to the client
            if self.transmit_client(sendobj) == -1:
                return -1

        return 0
       
    def send_released(self):
        while 1:
            try:
                obj = self.releasedlive.get(False)
            except StdQueue.Empty:
                return 0
            except EOFError:
                return -1

            sendobj = obj[0] + obj[1]
            # Just reflect the objects directly to the client
            if self.transmit_client(sendobj) == -1:
                return -1
        
        return 0    
        
    def transmit_client(self, result):
        try:
            sent = self.sock.send(result)
        except error, msg:
            log("Error sending message to client fd %d: %s" % (self.sock.fileno(), msg[1]))
            return -1

        if sent == 0:
            return -1

        if (sent < len(result)):
            self.outstanding = result[sent:]
        else:
            self.outstanding = ""
        return 0
        

    def receive_worker(self):

        # Only deal with one worker result at a time - we want to
        # prioritise live data over processing history
        while 1:

            # If we haven't finished a previous transmit, finish that off
            # first before fetching new results
            if len(self.outstanding) > 0:
                return self.transmit_client(self.outstanding)

            try:
                obj = self.workdone.get(False)
            except StdQueue.Empty:
                return 0

            response = obj[0]
            result = obj[1]

            # A worker has completed a job, let's form up a response to
            # send to our client
            if response in [NNTSC_COLLECTIONS, NNTSC_SCHEMAS, NNTSC_STREAMS, \
                        NNTSC_HISTORY, NNTSC_ACTIVE_STREAMS, \
                        NNTSC_QUERY_CANCELLED]:
                return self.transmit_client(result)

            elif response == NNTSC_REGISTER_COLLECTION:
                self.parent.register_collection(self.sock, result)
                    
            elif response == NNTSC_SUBSCRIBE:
                self.subscribe_stream(result)
            elif response == NNTSC_HISTORY_DONE:
                label = obj[1]
                timestamp = obj[2]
                if self.finish_subscribe(label, timestamp) == -1:
                    return -1
            
            else:
                # Response type was invalid
                log("Received invalid response from worker thread: %d" % (response))
                return -1

        return 0

    def write_required(self):
        if len(self.outstanding) > 0:
            return True
        if self.workdone.qsize() > 0:
            return True
        if self.livequeue.qsize() > 0:
            return True
        if self.releasedlive.qsize() > 0:
            return True

        return False

    def disconnect(self):
        self.running = 0


    def run(self):
        self.running = 1

        # Tell the client what version of the client API they need
        contents = pickle.dumps(NNTSC_CLIENTAPI_VERSION)
        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_VERSION_CHECK, 
                len(contents))

        self.transmit_client(header + contents)

        while self.running:
            input = [self.sock]
            
            if self.write_required():
                writer = [self.sock]
            else:
                writer = []

            # Timeout of zero is bad, will use lots of CPU. Hopefully, 0.01
            # won't keep us from serving the live queue for too long.
            inpready, outready, exready = select.select(input, writer, [], 0.01)
            for s in inpready:
                if s == self.sock:
                    if self.receive_client() == 0:
                        self.running = 0
                        break

            if self.sock in outready:
                if len(self.outstanding) > 0:
                    if self.transmit_client(self.outstanding) == -1:
                        self.running = 0
                        break
                elif self.releasedlive.qsize() > 0:
                    if self.send_released() == -1:
                        self.running = 0
                        break
                elif self.livequeue.qsize() > 0:
                    if self.receive_live() == -1:
                        self.running = 0
                        break
                elif self.receive_worker() == -1:
                    self.running = 0
                    break


        #log("Closing client thread on fd %d" % self.sock.fileno())
        self.parent.deregister_client(self.sock)
        self.livequeue.close()
        self.sock.close()

        # Add "halt" jobs to the job queue for each worker
        for w in self.workers:
            self.jobs.put((-1, None), True, 60)

class NNTSCExporter:
    def __init__(self, port):
        self.listen_port = port
        self.listen_sock = None
        self.dbconf = None
        self.collections = {}
        self.subscribers = {}
        self.sources = []
        self.livequeue = None

        self.clients = {}
        self.clientlock = threading.Lock()
        self.sublock = threading.Lock()


    def deregister_client(self, sock):

        self.clientlock.acquire()
        if sock in self.clients:
            del self.clients[sock]
        self.clientlock.release()

    def drop_source(self, key):
        log("Dropping source on queue %s" % (key))
        if self.livequeue:
            self.livequeue.unbind_queue(key)

    def filter_columns(self, cols, data, stream_id, timestamp):

        # Filter the data received from the NNTSC data parser to only
        # contain the columns that were asked for by the client

        # Always need these
        results = {"label":stream_id, "timestamp":timestamp}

        for k, v in data.items():
            if k in cols:
                results[k] = v

        return results

    def register_stream(self, streams, sock, cols, start, end, name, label):

        self.sublock.acquire()

        for s in streams:
            if self.subscribers.has_key(s):
                self.subscribers[s].append((sock, cols, start, end, name))
            else:
                self.subscribers[s] = [(sock, cols, start, end, name)]
            
            if name in self.collections and sock in self.collections[name]:
                self.collections[name][sock] += 1
            #log("Registered stream %d for socket %d" % (s, sock.fileno()))
        self.sublock.release()     


    def register_collection(self, sock, col):
        self.sublock.acquire()
        if self.collections.has_key(col):
            if sock not in self.collections[col]:
                self.collections[col][sock] = 0
        else:
            self.collections[col] = {sock: 0}
        #log("Registered collection %s for socket %d" % (col, sock.fileno()))
        self.sublock.release()

    def export_push(self, received, key):
        try:
            collid, timestamp = received
        except ValueError:
            log("Incorrect data format from source %s" % (key))
            log("Format should be (colid, timestamp)")
            self.drop_source(key)
            return -1
        
        pushdata = pickle.dumps((collid, timestamp))
        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_PUSH, len(pushdata))

        self.sublock.acquire()
        # Only export PUSH if someone is subscribed to a stream from the
        # collection in question
        if collid not in self.collections:
            self.sublock.release()
            return 0

        active = {}
        self.clientlock.acquire()
        for sock, subbed in self.collections[collid].items():
            if sock not in self.clients.keys():
                continue
            if subbed == 0:
                active[sock] = subbed
                continue
            
            try:
                self.clients[sock]['queue'].put((header, pushdata), True, 10)
            except StdQueue.Full:
                # This may not actually stop the client thread,
                # but apparently killing threads is bad so let's
                # hope the thread picks up that we closed its
                # socket and can exit itself nicely
                sock.close()
                #self.deregister_client(sock)
                continue


            active[sock] = subbed
        self.clientlock.release()
        self.collections[collid] = active
        self.sublock.release()

        return 0

    def export_new_stream(self, received, key):

        try:
            coll_id, coll_name, stream_id, properties = received
        except ValueError:
            log("Incorrect data format from source %s" % (key))
            log("Format should be (collection id, collection name, streamid, values dict)")
            return -1

        if not isinstance(properties, dict):
            log("Values should expressed as a dictionary")
            return -1
        
        properties['stream_id'] = stream_id

        ns_data = pickle.dumps((coll_id, False, [properties]))
        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_STREAMS,
                len(ns_data))

        self.sublock.acquire()
        if coll_id not in self.collections.keys():
            self.sublock.release()
            return 0

        #log("Exporting new stream %d to interested clients" % (stream_id))

        active = {}
        
        self.clientlock.acquire()
        for sock, subbed in self.collections[coll_id].items():
            if sock not in self.clients.keys():
                continue
            try:
                self.clients[sock]['queue'].put((header, ns_data), True, 10)
            except StdQueue.Full:
                # This may not actually stop the client thread,
                # but apparently killing threads is bad so let's
                # hope the thread picks up that we closed its
                # socket and can exit itself nicely
                sock.close()
                #self.deregister_client(sock)
                continue
            
            active[sock] = subbed
        self.clientlock.release()
        self.collections[coll_id] = active
        self.sublock.release()
        return 0

    def export_live_data(self, received, key):

        try:
            name, stream_id, timestamp, values = received
        except ValueError:
            log("Incorrect data format from source %s" % (key))
            log("Format should be (name, streamid, timestamp, values dict)")
            return -1

        if not isinstance(values, dict):
            log("Values should expressed as a dictionary")
            return -1

        self.sublock.acquire()
        self.clientlock.acquire()
        if stream_id in self.subscribers.keys():
            active = []

            for sock, columns, start, end, col in self.subscribers[stream_id]:
                if timestamp < start:
                    active.append((sock, columns, start, end, col))
                    continue

                if end != None and timestamp > end:
                    results = {}
                else:
                    results = self.filter_columns(columns, values, stream_id,
                            timestamp)

                contents = pickle.dumps((col, stream_id, results))
                header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_LIVE,
                        len(contents))

                if sock in self.clients.keys():
                    try:
                        self.clients[sock]['queue'].put((header, contents, timestamp), True, 10)
                    except StdQueue.Full:
                        # This may not actually stop the client thread,
                        # but apparently killing threads is bad so let's
                        # hope the thread picks up that we closed its
                        # socket and can exit itself nicely
                        sock.close()
                        #self.deregister_client(sock)
                        continue
                    #log("Pushed live data onto queue for stream %d" % (stream_id))
                    if results != {}:
                        active.append((sock, columns, start, end, col))
            self.subscribers[stream_id] = active
        self.clientlock.release()
        self.sublock.release()

        return 0

    def receive_source(self, channel, method, properties, body):

        key = method.routing_key

        msgtype, contents = pickle.loads(body)

        if msgtype == 0:
            ret = self.export_live_data(contents, key)
        if msgtype == 1:
            ret = self.export_new_stream(contents, key)
        if msgtype == 2:
            ret = self.export_push(contents, key)

        channel.basic_ack(delivery_tag = method.delivery_tag)
        if ret == -1:
            self.drop_source(key)
    
        return ret

    def createClient(self, clientfd): 
        #log("Creating client on fd %d" % (clientfd.fileno()))
        queue = Queue(1000000)
        clientfd.setblocking(0)

        cthread = NNTSCClient(clientfd, self, queue, self.dbconf, \
                self.dbtimeout)
        cthread.daemon = True
        cthread.start()

        self.clientlock.acquire()
        self.clients[clientfd] = {'queue':queue, 'thread':cthread}
        self.clientlock.release()


    def register_source(self, key, queue):
        log("Registering source on key %s" % (key))

        if key not in self.sources:
            self.sources.append(key)

    def create_listener(self, port):

        try:
            s = socket(AF_INET, SOCK_STREAM)
        except error, msg:
            log("Failed to create socket: %s" % (msg[1]))
            return -1

        try:
            s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        except error, msg:
            log("Failed to set SO_REUSEADDR: %s" % (msg[1]))
            return -1

        try:
            s.bind(('', port))
        except error, msg:
            log("Failed to bind to port %d: %s" % (port, msg[1]))
            return -1

        try:
            s.listen(5)
        except error, msg:
            log("Failed to listen on port %d: %s" % (port, msg[1]))
            return -1

        return s

    def configure(self, conf_fname, dbtimeout, queueid):
        nntsc_conf = load_nntsc_config(conf_fname)
        if nntsc_conf == 0:
            sys.exit(0)

        dbconf = get_nntsc_db_config(nntsc_conf)
        if dbconf == {}:
            sys.exit(1)

        self.dbconf = dbconf
        self.dbtimeout = dbtimeout

        self.listen_sock = self.create_listener(self.listen_port)

        if self.listen_sock == -1:
            return -1

        if queueid != None:
            self.livequeue = initExportConsumer(nntsc_conf, queueid, 
                    'nntsclive')
       
            if self.livequeue == None:
                log("Failed to initialise consumer for exporter")
                return -1

        return 0 

    def run(self):
        # Start up our listener thread
        if self.listen_sock == None:
            log("Must successfully call configure before calling run on the exporter!")
            return
        
        listenthread = NNTSCListener(self.listen_sock, self)
        listenthread.daemon = True
        listenthread.start()
       
        if self.livequeue is not None:
         
            # Prepare our export queue consumer
            while 1:
                for s in self.sources:
                    self.livequeue.bind_queue(s)
                self.livequeue.configure_consumer(self.receive_source)

                # Start reading from the queue
                retval = self.livequeue.run_consumer()
                if retval == PIKA_CONSUMER_HALT:
                    break

                # Reconnect if we are going to be retrying
                self.livequeue.connect()
        else:
            while 1:
                try:
                    listenthread.join(1)
                except KeyboardInterrupt:
                    break
                except:
                    raise
        


class NNTSCListener(threading.Thread):
    def __init__(self, sock, parent):
        threading.Thread.__init__(self)
        self.parent = parent
        self.sock = sock

    def run(self):
        while 1:
            # Wait for any sign of an incoming connection
            listener = [self.sock]

            inpready, outready, exready = select.select(listener, [], [])
            
            if self.sock not in inpready:
                continue
            
            # Accept the connection
            try:
                client, addr = self.sock.accept()
            except error, msg:
                log("Error accepting connection: %s" % (msg[1]))
                break

            # Create a new client entry
            self.parent.createClient(client)


if __name__ == '__main__':

    opts, rest = getopt.getopt(sys.argv[1:],'p:h')

    for o, a in opts:
        if o == "-p":
            listen_port = int(a)
        if o == '-h':
            print_usage(sys.argv[0])

    exp = NNTSCExporter(listen_port)
    exp.configure(rest[0])
    exp.run()




# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :

