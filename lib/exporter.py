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
from pywandevent.pywandevent import PyWandEvent
from socket import *
from multiprocessing import Pipe
import threading, select

from libnntsc.dbselect import DBSelector
from libnntsc.configurator import *
from libnntscclient.protocol import *
from libnntscclient.logger import *

# There are 3 classes defined in this file that form a hierarchy for
# exporting live and/or historical data to clients.
#
# The hierarchy is as follows:
#
# NNTSC has 1 instance of the NNTSCExporter class. It listens for new
# client connections and ensures that any new live data is passed on
# to any clients that have subscribed to it. The NNTSC dataparsers
# register themselves as sources to the NNTSCExporter on start-up and
# send all new data to the exporter via a pipe. If this pipe fills up,
# the dataparser can no longer deal with new measurements so it is
# *vital* that this pipe is not ignored for any length of time.
#
# When a new client connects, a new instance of NNTSCClient is created
# which runs in a separate thread. This thread deals with reading requests
# for data from the client and sending
# the responses back to the client. It also handles forwarding any live
# data that the NNTSCExporter passes on to it -- this particular task
# has higher priority than the others. The code for packing and unpacking
# NNTSC protocol messages is all contained within this class.
#
# Whenever a NNTSCClient receives a request for data that requires a
# database query, e.g. historical data or a list of streams, the job is
# farmed out to an instance of the DBWorker class. Each NNTSCClient will
# own a number of DBWorker threads (equal to MAX_WORKERS) which can
# query the database without affecting processing of live data or new
# client requests. When the query completes, the results are returned
# back to the NNTSCClient instance via yet another pipe for subsequent
# transmission to the client that requested them. Before doing so, the
# DBWorker thread will also run over the results, transforming them into
# a nice dictionary mapping column names to values and estimating the
# measurement frequency (which is required by most client applications).
#
# In summary:
#   1 NNTSCExporter
#   1 NNTSCClient thread per connected client
#   MAX_WORKERS DBWorker threads per NNTSCClient

MAX_HISTORY_QUERY = (24 * 60 * 60 * 7)
MAX_WORKERS = 2

class DBWorker(threading.Thread):
    def __init__(self, parent, pipeend, dbconf, lock, cv, threadid):
        threading.Thread.__init__(self)
        self.dbconf = dbconf
        self.parent = parent
        self.pipeend = pipeend
        self.lock = lock
        self.cv = cv
        self.threadid = threadid

    def process_job(self, job):
        jobtype = job[0]
        jobdata = job[1]

        if jobtype == -1:
            return -1

        if jobtype == NNTSC_REQUEST:
            return self.process_request(jobdata)

        if jobtype == NNTSC_AGGREGATE:
            return self.aggregate(jobdata)

        if jobtype == NNTSC_PERCENTILE:
            return self.percentile(jobdata)

        if jobtype == NNTSC_SUBSCRIBE:
            return self.subscribe(jobdata)

        return -1

    def subscribe_streams(self, streams, start, end, cols, name):
        for s in streams:
            try:
                self.pipeend.send((NNTSC_SUBSCRIBE, \
                        (s, start, end, cols, name)))
            except IOError as e:
                log("Failed to subscribe to %s: %s" % (s, e))
                return -1

        return 0

    def aggregate(self, aggmsg):
        tup = pickle.loads(aggmsg)
        name, start, end, labels, aggcols, groupcols, binsize, aggfunc = tup
        now = int(time.time())

        if end == 0:
            end = None

        if start == None or start >= now:
            # No historical data, send empty history for all streams
            for lab, streams in labels.items():
                result = ({lab:[]}, {lab:0}, name, [lab], "raw", False)
                try:
                    self.pipeend.send((NNTSC_HISTORY, result))
                except IOError as e:
                    log("Failed to return empty history: %s\n" % (e))
                    return -1
                    
            return 0

        if end == None:
            stoppoint = int(time.time())
        else:
            stoppoint = end

        while start < stoppoint:
            queryend = start + MAX_HISTORY_QUERY

            if queryend >= stoppoint:
                queryend = stoppoint
                more = False
            else:
                more = True

            generator = self.db.select_aggregated_data(name, labels, aggcols,
                    start, queryend, groupcols, binsize, aggfunc)

            if self._query_history(generator, name, start, queryend,
                    labels, [], more, -1) == -1:
                return -1
            start = queryend + 1

            # If we were asked for data up until "now", make sure we account
            # for the time taken to make earlier queries otherwise we'll
            # miss any new data inserted while we were querying previous
            # weeks of data
            if end == None:
                stoppoint = int(time.time())

        return 0

    def percentile(self, pcntmsg):
        tup = pickle.loads(pcntmsg)
        name, start, end, labels, binsize, ntilecols, othercols, \
                ntileagg, otheragg = tup
        now = int(time.time())

        if end == 0:
            end = None

        if start == None or start >= now:
            # No historical data, send empty history for all streams
            for lab, streams in labels.items():
                result = ({lab:[]}, {lab:0}, name, [lab], "raw", False)
                try:
                    self.pipeend.send((NNTSC_HISTORY, result))
                except IOError as e:
                    log("Failed to return empty history: %s\n" % (e))
                    return -1
                    
            return 0

        if end == None:
            stoppoint = int(time.time())
        else:
            stoppoint = end

        while start < stoppoint:
            queryend = start + MAX_HISTORY_QUERY

            if queryend >= stoppoint:
                queryend = stoppoint
                more = False
            else:
                more = True

            generator = self.db.select_percentile_data(name, labels, ntilecols,
                    othercols, start, queryend, binsize, ntileagg, otheragg)

            if self._query_history(generator, name, start, queryend,
                    labels, [], more, -1) == -1:
                return -1
            start = queryend + 1

            # If we were asked for data up until "now", make sure we account
            # for the time taken to make earlier queries otherwise we'll
            # miss any new data inserted while we were querying previous
            # weeks of data
            if end == None:
                stoppoint = int(time.time())

        return 0

    def subscribe(self, submsg):
        name, start, end, cols, labels, aggs = pickle.loads(submsg)
        now = int(time.time())

        if start == 0 or start == None:
            start = now
        if end == 0:
            end = None

        if (end == None or end > now):
            subend = end
        else:
            subend = -1

        if start >= now:
            # No historical data, send empty history for all streams
            for lab, streams in labels.items():
                result = ({lab:[]}, {lab:0}, name, [lab], "raw", False)
                try:
                    self.pipeend.send((NNTSC_HISTORY, result))
                except IOError as e:
                    log("Failed to return empty history: %s\n" % (e))
                    return -1

                if self.subscribe_streams(streams, start, end, cols, name) == -1:
                    return -1
                    
            return 0

        if end == None:
            stoppoint = int(time.time())
        else:
            stoppoint = end

        while start < stoppoint:
            queryend = start + MAX_HISTORY_QUERY
    
            # If we were asked for data up until "now", make sure we account
            # for the time taken to make earlier queries otherwise we'll
            # miss any new data inserted while we were querying previous
            # weeks of data
            if end == None:
                stoppoint = int(time.time())

            if queryend >= stoppoint:
                queryend = stoppoint
                more = False
            else:
                more = True

            # Only aggregate the streams for each label if explicitly requested,
            # otherwise fetch full historical data
            
            if aggs != []:
                generator = self.db.select_aggregated_data(name, labels, 
                        cols, start, queryend, [], 1, aggs)
            else:
                generator = self.db.select_data(name, labels, cols, start, 
                        queryend)

            if (self._query_history(generator, name, start, queryend,
                    labels, cols, more, subend)) == -1:
                return -1

            # Don't subscribe more than once
            if more == False:
                subend = -1

            start = queryend + 1

        log("Subscribe job completed successfully (%s)\n" % (self.threadid))
        return 0

    def _query_history(self, rowgen, name, start, end, labels, cols,
            more, subend):

        currlabel = -1
        historysize = 0

        observed = set([])

        # Get any historical data that we've been asked for
        for row, tscol, binsize in rowgen:

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
                    result = ({ currlabel : history },
                            { currlabel : freq },
                            name, [currlabel], "raw", thismore)

                    assert(currlabel in labels)
                    if self._write_history(labels[currlabel], result, name, 
                                cols,
                            start, subend, thismore) == -1:
                        return -1

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
            for k,v in row.items():
                datadict[k] = v
            history.append(datadict)
            historysize += 1

        if historysize != 0:
            # Make sure we write out the last stream
            freq = self._calc_frequency(freqstats, binsize)
            result = ({ currlabel : history },
                    { currlabel : freq },
                    name, [currlabel], "raw", more)
            if self._write_history(labels[currlabel], result, name, cols,
                    start, subend, more) == -1:
                return -1

        # Also remember to export empty history for any streams that had
        # no data in the request period
        # XXX convert to string to work temporarily with old style stream_ids
        allstreams = set(labels.keys())

        missing = allstreams - observed
        for m in missing:
            result = ({m : []}, {m : 0}, name, [m], "raw", more)
            assert (m in labels)
            if self._write_history(labels[m], result, name, cols,
                    start, subend, more) == -1:
                return -1

        return 0

    # Nice little helper function that pushes history data onto the pipe
    # back to our NNTSCClient
    def _write_history(self, streams, result, name, cols, start, subend, more):
        try:
            self.pipeend.send((NNTSC_HISTORY, result))
        except IOError as e:
            log("Failed to return history to client: %s" % (e))
            return -1

        if subend != -1 and not more:
            if self.subscribe_streams(streams, start, subend, cols, name) == -1:
                return -1

        return 0

    # Processes the job for a basic NNTSC request, i.e. asking for the
    # collections, schemas or streams rather than querying for time
    # series data
    def process_request(self, reqmsg):
        req_hdr = struct.unpack(nntsc_req_fmt,
                reqmsg[0:struct.calcsize(nntsc_req_fmt)])

        if req_hdr[0] == NNTSC_REQ_COLLECTION:
            # Requesting the collection list
            cols = self.db.list_collections()

            shrink = []
            for c in cols:
                shrink.append({"id":c['id'], "module":c['module'], "modsubtype":c['modsubtype']})

            try:
                self.pipeend.send((NNTSC_COLLECTIONS, shrink))
            except IOError as e:
                log("Sending collections failed: %s" % (e))
                return -1

        if req_hdr[0] == NNTSC_REQ_SCHEMA:
            col_id = req_hdr[1]
            stream_schema, data_schema = self.db.get_collection_schema(col_id)

            result = (col_id, stream_schema, data_schema)
            try:
                self.pipeend.send((NNTSC_SCHEMAS, result ))
            except IOError as e:
                log("Sending schemas failed: %s" % (e))
                return -1


        if req_hdr[0] == NNTSC_REQ_STREAMS:
            col = req_hdr[1]
            startstream = req_hdr[2]
            streams = self.db.select_streams_by_collection(col, startstream)
            try:
                self.pipeend.send((NNTSC_STREAMS, (col, streams)))
            except IOError as e:
                log("Sending streams failed: %s" % (e))
                return -1

        return 0

    def run(self):
        running = 1

        while running:
            self.cv.acquire()

            # Wait for a job to become available
            while len(self.parent.jobs) == 0:
                self.cv.wait()

            # Grab the first job available
            job = self.parent.jobs[0]
            self.parent.jobs = self.parent.jobs[1:]
            self.cv.release()

            # Don't have a db connection open for the lifetime of
            # this thread if we aren't using it otherwise we run the
            # risk of inactive threads preventing us from contacting
            # the database
            self.db = DBSelector(self.threadid, self.dbconf["name"], self.dbconf["user"],
                    self.dbconf["pass"], self.dbconf["host"])
            if self.process_job(job) == -1:
                break
            self.db.close()

        # Thread is over, tidy up
        self.db.close()
        self.pipeend.close()

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
    def __init__(self, sock, parent, pipeend, dbconf):
        threading.Thread.__init__(self)
        assert(dbconf)
        self.sock = sock
        self.parent = parent
        self.pipeend = pipeend
        self.recvbuf = ""
        self.jobs = []
        self.joblock = threading.Lock()
        self.jobcv = threading.Condition(self.joblock)

        self.workers = []
        # Create some worker threads for handling the database queries
        for i in range(0, MAX_WORKERS):
            pipe_recv, pipe_send = Pipe(False)
            threadid = "client%d_thread%d" % (self.sock.fileno(), i)

            worker = DBWorker(self, pipe_send, dbconf, self.joblock,
                    self.jobcv, threadid)
            worker.daemon = True
            worker.start()

            self.workers.append((pipe_recv, worker, pipe_send))


    def export_hist_block(self, name, streamid, block, more, freq,
            aggname):

        contents = pickle.dumps((name, streamid, block, more, freq, aggname))
        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_HISTORY, len(contents))

        try:
            self.sock.send(header + contents)
        except error, msg:
            log("Error sending data to client fd %d: %s" % (self.sock.fileno(), msg[1]))
            return -1

        return 0

    def send_collections(self, cols):

        col_pickle = pickle.dumps(cols)
        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_COLLECTIONS, len(col_pickle))
        try:
            self.sock.send(header + col_pickle)
        except error, msg:
            log("Error sending collections to client fd %d" % (self.sock.fileno(), msg[1]))
            return -1
        return 0

    def send_schema(self, schema):

        schema_pick = pickle.dumps(schema)
        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_SCHEMAS, len(schema_pick))

        try:
            self.sock.send(header + schema_pick)
        except error, msg:
            log("Error sending schemas to client fd %d: %s" % (self.sock.fileno(), msg[1]))
            return -1
        return 0

    def export_streams_msg(self, col, more, streams):
        stream_data = pickle.dumps((col, more, streams))

        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_STREAMS,
                len(stream_data))
        try:
            self.sock.send(header + stream_data)
        except error, msg:
            log("Error sending schemas to client fd %d: %s" % (self.sock.fileno(), msg[1]))
            return -1

        return 0

    def send_streams(self, streamresult):

        col = streamresult[0]
        streams = streamresult[1]
        self.parent.register_collection(self.sock, col)

        if len(streams) == 0:
            return self.export_streams_msg(col, False, [])

        i = 0
        while (i < len(streams)):

            start = i
            if len(streams) <= i + 1000:
                end = len(streams)
                more = False
            else:
                end = i + 1000
                more = True

            if self.export_streams_msg(col, more, streams[start:end]) == -1:
                return -1

            i = end
        return 0

    def send_history(self, subresult):
        history, freq, name, streams, aggfunc, more = subresult
        now = int(time.time())

        for s in streams:

            # Send the history for this stream
            if s in history:
                assert(s in freq)
                if self.export_hist_block(name, s, history[s], more, freq[s],
                            aggfunc) == -1:
                    return -1
            else:
                # No history, send an empty list so our client doesn't get
                # stuck waiting for the data
                if self.export_hist_block(name, s, [], False, 0, aggfunc) == -1:
                    return -1
        return 0

    def subscribe_stream(self, submsg):
        stream, start, end, cols, name = submsg
        self.parent.register_stream(stream, self.sock, cols, start, end, name)

    def client_message(self, msg):
        error = 0
        header = struct.unpack(nntsc_hdr_fmt, msg[0:struct.calcsize(nntsc_hdr_fmt)])
        total_len = header[2] + struct.calcsize(nntsc_hdr_fmt)
        body = msg[struct.calcsize(nntsc_hdr_fmt):total_len]

        if len(msg) < total_len:
            return msg, 1, error

        # Put the job on our joblist for one of the worker threads to handle
        self.jobcv.acquire()
        self.jobs.append((header[1], body))
        self.jobcv.notify()
        self.jobcv.release()

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
        fd = self.pipeend.fileno()

        try:
            obj = self.pipeend.recv()
        except EOFError, msg:
            log("Error receiving live data from pipe %d: %s" % (fd, msg))
            return 0

        # Just reflect whatever we get on the socket to our client
        totalsent = 0
        while totalsent < len(obj):
                sent = self.sock.send(obj[totalsent:])
                if sent == 0:
                    return 0
                totalsent += sent

        return totalsent

    def receive_worker(self, socket):
        fd = socket.fileno()

        try:
            response, result = socket.recv()
        except EOFError as msg:
            log("Error receiving query result from DBWorker %d: %s" % (fd, msg))
            return 0

        # A worker has completed a job, let's form up a response to
        # send to our client
        if response == NNTSC_COLLECTIONS:
            return self.send_collections(result)

        if response == NNTSC_SCHEMAS:
            return self.send_schema(result)

        if response == NNTSC_STREAMS:
            return self.send_streams(result)

        if response == NNTSC_HISTORY:
            return self.send_history(result)

        if response == NNTSC_SUBSCRIBE:
            return self.subscribe_stream(result)

        # Response type was invalid
        return -1

    def run(self):
        running = 1

        while running:
            input = [self.sock, self.pipeend]

            for w in self.workers:
                input.append(w[0])

            inpready, outready, exready = select.select(input, [], [])
            for s in inpready:
                if s == self.pipeend:
                    if self.receive_live() == 0:
                        running = 0
                        break
                    # XXX This continue means that we prefer reading live
                    # data -- this may be a bad idea
                    continue
                elif s == self.sock:
                    if self.receive_client() == 0:
                        running = 0
                        break
                else:
                    # Must be a query result from a DBWorker
                    if self.receive_worker(s) == -1:
                        running = 0
                        break
        #log("Closing client thread on fd %d" % self.sock.fileno())
        self.parent.deregister_client(self.sock)
        self.pipeend.close()
        self.sock.close()

        # Add "halt" jobs to the job queue for each worker
        self.jobcv.acquire()
        for w in self.workers:
            self.jobs.append((-1, None))
        self.jobcv.notify_all()
        self.jobcv.release()

        # Make sure we close our end of the pipe to each thread
        for w in self.workers:
            w[0].close()

class NNTSCExporter:
    def __init__(self, port):
        self.pwe = PyWandEvent()

        self.listen_port = port
        self.dbconf = None
        self.collections = {}
        self.subscribers = {}
        self.client_sockets = {}
        self.client_threads = {}

    def deregister_client(self, sock):

        #log("Dropping client on fd %d" % (sock.fileno()))
        if sock in self.client_sockets:
            del self.client_sockets[sock]
        if sock in self.client_threads:
            del self.client_threads[sock]

    def drop_source(self, sock):
        log("Dropping source on fd %d" % (sock.fileno()))

        self.pwe.del_fd_event(sock)
        sock.close()

    def filter_columns(self, cols, data, stream_id, ts):

        # Filter the data received from the NNTSC data parser to only
        # contain the columns that were asked for by the client

        # Always need these
        results = {"label":stream_id, "timestamp":ts}

        for k,v in data.items():
            if k in cols:
                results[k] = v

        return results

    def register_stream(self, s, sock, cols, start, end, name):
        #print "Registered new stream: ", s, start, end
        if self.subscribers.has_key(s):
            self.subscribers[s].append((sock, cols, start, end, name))
        else:
            self.subscribers[s] = [(sock, cols, start, end, name)]


    def register_collection(self, sock, col):
        if self.collections.has_key(col):
            if sock not in self.collections[col]:
                self.collections[col].append(sock)
        else:
            self.collections[col] = [sock]
    
    def export_push(self, received, fd):
        try:
            collid, timestamp = received
        except ValueError:
            log("Incorrect data format from source %d" % (fd))
            log("Format should be (colid, timestamp)")
            self.drop_source(sock)
            return

        # Only export PUSH if someone is subscribed to a stream from the
        # collection in question
        if collid not in self.collections:
            return 0

        pushdata = pickle.dumps((collid, timestamp))
        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_PUSH, len(pushdata))

        active = []
        for sock in self.collections[collid]:
            if sock not in self.client_sockets:
                continue

            pipesend = self.client_sockets[sock]
            try:
                pipesend.send(header + pushdata)
            except error, msg:
                log("Error sending push to pipe for client fd %d: %s" % (sock.fileno(), msg[1]))
                self.deregister_client(sock)
            else:
                active.append(sock)
        self.collections[collid] = active

        return 0

    def export_new_stream(self, received, fd):

        try:
            coll_id, coll_name, stream_id, properties = received
        except ValueError:
            log("Incorrect data format from source %d" % (fd))
            log("Format should be (collection id, collection name, streamid, values dict)")
            return -1

        if not isinstance(properties, dict):
            log("Values should expressed as a dictionary")
            return -1

        if coll_id not in self.collections.keys():
            return 0

        #log("Exporting new stream %d to interested clients" % (stream_id))
        properties['stream_id'] = stream_id

        ns_data = pickle.dumps((coll_id, False, [properties]))
        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_STREAMS,
                len(ns_data))

        active = []
        for sock in self.collections[coll_id]:
            if sock not in self.client_sockets:
                continue

            pipesend = self.client_sockets[sock]
            try:
                pipesend.send(header + ns_data)
            except error, msg:
                log("Error sending schemas to pipe for client fd %d: %s" % (sock.fileno(), msg[1]))
                self.deregister_client(sock)
            else:
                active.append(sock)
        self.collections[coll_id] = active

        return 0

    def export_live_data(self, received, fd):

        try:
            name, stream_id, timestamp, values = received
        except ValueError:
            log("Incorrect data format from source %d" % (fd))
            log("Format should be (name, streamid, timestamp, values dict)")
            self.drop_source(sock)
            return

        if not isinstance(values, dict):
            log("Values should expressed as a dictionary")
            self.drop_source(sock)
            return
   
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

                if sock in self.client_sockets:
                    pipesend = self.client_sockets[sock]

                    #print "Pushing onto client pipe", stream_id, col
                    pipesend.send(header + contents)

                    #    log("Error sending live data to pipe for client: %s" % (msg[1]))
                    #    self.deregister_client(sock)
                    #else:
                    if results != {}:
                        active.append((sock, columns, start, end, col))
            self.subscribers[stream_id] = active

        return 0

    def receive_source(self, fd, evtype, sock, data):

        try:
            obj = sock.recv()
        except EOFError, msg:
            log("Error receiving data from source %d: %s" % (fd, msg))
            self.drop_source(sock)
            return

        msgtype, contents = obj

        if msgtype == 0:
            ret = self.export_live_data(contents, sock.fileno())
        if msgtype == 1:
            ret = self.export_new_stream(contents, sock.fileno())
        if msgtype == 2:
            ret = self.export_push(contents, sock.fileno())

        return ret


    def accept_connection(self, fd, evtype, sock, data):

        try:
            client, addr = sock.accept()
        except error, msg:
            log("Error accepting connection: %s" % (msg[1]))
            return

        #log("Accepted connection on fd %d" % (client.fileno()))

        pipe_recv, pipe_send = Pipe(False)

        cthread = NNTSCClient(client, self, pipe_recv, self.dbconf)
        cthread.daemon = True
        cthread.start()
        self.client_sockets[client] = pipe_send
        self.client_threads[client] = cthread

        #self.pwe.add_fd_event(client, 1, "", self.receive_client)


    def register_source(self, pipe):
        log("Registering source on fd %d" % (pipe.fileno()))

        self.pwe.add_fd_event(pipe, 1, "", self.receive_source)

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

    def configure(self, conf_fname):
        nntsc_conf = load_nntsc_config(conf_fname)
        if nntsc_conf == 0:
            sys.exit(0)

        dbconf = get_nntsc_db_config(nntsc_conf)
        if dbconf == {}:
            sys.exit(1)

        self.dbconf = dbconf

        listen_sock = self.create_listener(self.listen_port)

        if listen_sock == -1:
            return -1

        self.pwe.add_fd_event(listen_sock, 1, None, self.accept_connection)

    def run(self):
        self.pwe.run()

if __name__ == '__main__':

    opts, rest = getopt.getopt(sys.argv[1:],'p:h')

    for o,a in opts:
        if o == "-p":
            listen_port = int(a)
        if o == '-h':
            print_usage(sys.argv[0])

    exp = NNTSCExporter(listen_port)
    exp.configure(rest[0])
    exp.run()




# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :

