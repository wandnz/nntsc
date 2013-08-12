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

from libnntsc.database import Database
from libnntsc.configurator import *
from libnntsc.export import *
from libnntsc.logger import *
        
class NNTSCClient(threading.Thread):
    def __init__(self, sock, parent, pipeend, dbconf):
        threading.Thread.__init__(self)
        assert(dbconf)
        self.sock = sock
        self.parent = parent
        self.pipeend = pipeend
        self.recvbuf = ""
        self.db = Database(dbconf["name"], dbconf["user"], dbconf["pass"],
                dbconf["host"])
    
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


    def send_history(self, streamid, name, hist, freq, aggname):
       
        tosend = []
        c = 0

        # If there is no data to send, just push out an empty list and
        # return.
        if len(hist) == 0:
            return self.export_hist_block(name, streamid, tosend,
                    False, freq, aggname)

        for h in hist:
            
            tosend.append(h)
            c += 1

            if (c >= 1000):
                if h != hist[-1]:
                    more = True
                else:
                    more = False

                if self.export_hist_block(name, streamid, tosend, 
                            more, freq, aggname) == -1:
                    return -1
                c = 0
                tosend = []

        if c != 0:
            return self.export_hist_block(name, streamid, tosend, False,
                    freq, aggname)
        return 0
                    
    def send_collections(self, cols):
        col_pickle = pickle.dumps(cols)
        msglen = len(col_pickle)

        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_COLLECTIONS, msglen)

        try:
            self.sock.send(header + col_pickle)
        except error, msg:
            log("Error sending collections to client fd %d" % (self.sock.fileno(), msg[1]))
            return -1
        return 0

    def send_schema(self, col_id):
        
        stream_schema, data_schema = self.db.get_collection_schema(col_id)

        # We just want the column names
        stream_schema = map(lambda a: a.name, stream_schema)
        data_schema = map(lambda a: a.name, data_schema)

        schema_pick = pickle.dumps((col_id, stream_schema, data_schema))

        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_SCHEMAS, len(schema_pick))

        try:
            self.sock.send(header + schema_pick)
        except error, msg:
            log("Error sending schemas to client fd %d" % (self.sock.fileno(), msg[1]))
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

    def send_streams(self, col, startstream):
     
        #log("Sending streams to client for collection %d" % (col)) 
        self.parent.register_collection(self.sock, col)
       
        streams = self.db.select_streams_by_collection(col, startstream)
      
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
    
    def subscribe(self, submsg):
        name, start, end, cols, streams = pickle.loads(submsg)
        now = int(time.time())

        if start == 0 or start == None:
            start = now
        if end == 0:
            end = None
       
        for s in streams:

            # If live data is going to be required, add to the sub list
            if (end == None or end > now):
                self.parent.register_stream(s, self.sock, cols, start, end, name)

            # Send any historical data that we've been asked for
            if start < now:
                hist, freq = self.db.select_data(name, [s], cols, start, end)
                if self.send_history(s, name, hist, freq, "raw") == -1:
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

            return self.send_collections(shrink)

        if req_hdr[0] == NNTSC_REQ_SCHEMA:
            col_id = req_hdr[1]
            return self.send_schema(col_id)


        if req_hdr[0] == NNTSC_REQ_STREAMS:
            col_id = req_hdr[1]
            start = req_hdr[2]
            return self.send_streams(col_id, start)

        return 0
    
    def aggregate(self, aggmsg):
        tup = pickle.loads(aggmsg)
        name, start, end, streams, aggcols, groupcols, binsize, fname = tup
        now = int(time.time())

        if start == None:
            return 0
        if start > now:
            return 0
        if end == 0:
            end = None

        for s in streams:
            agghist, freq = self.db.select_aggregated_data(name, [s], aggcols, 
                    start, end, groupcols, binsize, fname)
            if self.send_history(s, name, agghist, freq, fname) == -1:
                return -1

        return 0

       
    def client_message(self, msg):

        error = 0
        header = struct.unpack(nntsc_hdr_fmt, msg[0:struct.calcsize(nntsc_hdr_fmt)])
        total_len = header[2] + struct.calcsize(nntsc_hdr_fmt)
        body = msg[struct.calcsize(nntsc_hdr_fmt):total_len]

        if len(msg) < total_len:
            return msg, 1, error

        if header[1] == NNTSC_REQUEST:
            if self.process_request(body) == -1:
                error = 1

        if header[1] == NNTSC_AGGREGATE:
            if self.aggregate(body) == -1:
                error = 1

        if header[1] == NNTSC_SUBSCRIBE:
            if self.subscribe(body) == -1:
                error = 1

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

    def run(self):
        running = 1
        
        while running:
            input = [self.sock, self.pipeend]

            inpready, outready, exready = select.select(input, [], [])
            for s in inpready:
                if s == self.sock:
                    if self.receive_client() == 0:
                        running = 0
                        break
                if s == self.pipeend:
                    if self.receive_live() == 0:
                        running = 0
                        break
        
        #log("Closing client thread on fd %d" % self.sock.fileno())
        self.parent.deregister_client(self.sock)
        self.pipeend.close()
        self.sock.close()


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
        del self.client_sockets[sock]
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

                    try:
                        pipesend.send(header + contents)
                    except error, msg:
                        log("Error sending live data to pipe for client %d: %s" % (sock.fileno(), msg[1]))
                        self.deregister_client(sock)
                    else:
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

