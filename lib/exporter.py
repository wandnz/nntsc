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

pipefill = 0
MAX_HISTORY_QUERY = (24 * 60 * 60 * 7 * 4)
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
   
    def aggregate(self, aggmsg):
        tup = pickle.loads(aggmsg)
        name, start, end, streams, aggcols, groupcols, binsize, aggfunc = tup
        now = int(time.time())
        
        if end == 0:
            end = None

        if start == None or start >= now:
            for s in streams:
                result = ({s:[]}, {s:0}, name, [s], fname, False)
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

            generator = self.db.select_aggregated_data(name, streams, aggcols, 
                    start, end, groupcols, binsize, aggfunc)

            if self._query_history(generator, name, start, queryend, 
                    streams, [], more, -1) == -1:
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
        name, start, end, streams, binsize, ntilecols, othercols, \
                ntileagg, otheragg = tup
        now = int(time.time())

        if end == 0:
            end = None

        if start == None or start >= now:
            for s in streams:
                result = ({s:[]}, {s:0}, name, [s], ntileagg, False)
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

            generator = self.db.select_percentile_data(name, streams, ntilecols, 
                    othercols, start, end, binsize, ntileagg, otheragg)

            if self._query_history(generator, name, start, queryend, 
                    streams, [], more, -1) == -1:
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
        name, start, end, cols, streams = pickle.loads(submsg)
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
            for s in streams:
                result = ({s:[]}, {s:0}, name, [s], "raw", sub)
                try:
                    self.pipeend.send((NNTSC_HISTORY, result))
                except IOError as e:
                    log("Failed to return empty history: %s\n" % (e))
                    return -1

                # Subscribe to all streams
                if sub:
                    try:
                        self.pipeend.send((NNTSC_SUBSCRIBE, \
                                (s, start, end, cols, name)))  
                    except IOError as e:
                        log("Failed to subscribe to %s: %s" % (s, e))
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

            generator = self.db.select_data(name, streams, cols, start, end)

            if (self._query_history(generator, name, start, queryend,
                    streams, cols, more, subend)) == -1:
                return -1

            # Don't subscribe to a stream more than once
            if subend != -1:
                subend = -1

            start = queryend + 1

            # If we were asked for data up until "now", make sure we account 
            # for the time taken to make earlier queries otherwise we'll 
            # miss any new data inserted while we were querying previous
            # weeks of data
            if end == None:
                stoppoint = int(time.time())

        log("Subscribe job completed successfully (%s)\n" % (self.threadid))
        return 0

    def _query_history(self, rowgen, name, start, end, streams, cols, 
            more, subend):

        currstream = -1
        historysize = 0

        observed = set([])

        # Get any historical data that we've been asked for
        for row, tscol, binsize in rowgen:
                        
            # Limit the amount of history we export at any given time
            # to prevent us from using too much memory during processing
            if row['stream_id'] != currstream or historysize > 10000:
                if currstream != -1:

                    # We've reached the history limit, so make sure we
                    # avoid duplicate subscriptions and set the 'more'
                    # flag correctly
                    if row['stream_id'] == currstream:
                        thissub = -1
                        thismore = True
                    else:
                        thissub = subend
                        thismore = more

                    # Export the history to the pipe
                    freq = self._calc_frequency(freqstats, binsize)
                    result = ({ currstream : history },
                            { currstream : freq },
                            name, [currstream], "raw", thismore)
                    if self._write_history(currstream, result, name, cols,
                            start, thissub) == -1:
                        return -1

                # Reset all our counters etc.
                freqstats = {'lastts': 0, 'lastbin':0, 'perfectbins':0,
                            'totaldiffs':0, 'tsdiffs':{} }
                currstream = row['stream_id']
                
                history = []
                historysize = 0
                observed.add(currstream)
      
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
            result = ({ currstream : history },
                    { currstream : freq },
                    name, [currstream], "raw", more)
            if self._write_history(currstream, result, name, cols,
                    start, subend) == -1:
                return -1
        # Also remember to export empty history for any streams that had
        # no data in the request period
        allstreams = set(streams)

        missing = allstreams - observed
        for m in missing:
            result = ({m : []}, {m : 0}, name, [m], "raw", more)
            if self._write_history(m, result, name, cols,
                    start, subend) == -1:
                return -1

        return 0
  
    def _write_history(self, stream, result, name, cols, start, subend):
        try:
            self.pipeend.send((NNTSC_HISTORY, result))
        except IOError as e:
            log("Failed to return history to client: %s" % (e))
            return -1
        
        if subend != -1:
            try:
                self.pipeend.send((NNTSC_SUBSCRIBE, \
                        (stream, start, subend, cols, name)))  
            except IOError as e:
                log("Subscribe failed for %s: %s" % (stream, e))
                return -1
      
        return 0
   
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

            while len(self.parent.jobs) == 0:
                self.cv.wait()

            job = self.parent.jobs[0]
            self.parent.jobs = self.parent.jobs[1:]
            self.cv.release()
           
            self.db = DBSelector(self.threadid, self.dbconf["name"], self.dbconf["user"], 
                    self.dbconf["pass"], self.dbconf["host"])
            if self.process_job(job) == -1:
                break
            self.db.close()

        self.db.close()
        self.pipeend.close()

    def _calc_frequency(self, freqdata, binsize):

        # No measurements were observed, use the binsize if sane
        if freqdata['totaldiffs'] == 0:
            if binsize < 300:
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
        global pipefill
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

        pipefill -= totalsent
        #print "Successfully reflected live data", pipefill
        return totalsent

    def receive_worker(self, socket):
        fd = socket.fileno()

        try:
            response, result = socket.recv()
        except EOFError as msg:
            log("Error receiving query result from DBWorker %d: %s" % (fd, msg))
            return 0

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

        assert(0)
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
        results = {"stream_id":stream_id, "timestamp":ts}

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
        global pipefill

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
                    pipefill += len(header + contents)
                    
                    #    log("Error sending live data to pipe for client: %s" % (msg[1]))
                    #    self.deregister_client(sock)
                    #else:
                    if results != {}:
                        active.append((sock, columns, start, end, col))
                    #print "Send successful", pipefill
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

