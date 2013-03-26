#!/usr/bin/env python

import sys, socket, time, struct, getopt, pickle
from pywandevent.pywandevent import PyWandEvent
from socket import *
from multiprocessing import Pipe

from libnntsc.database import Database
from libnntsc.configurator import *
from libnntsc.export import *

        
class NNTSCExporter:
    def __init__(self, port):
        self.pwe = PyWandEvent()

        self.listen_port = port
        self.db = None
        self.collections = {}
        self.subscribers = {}
        self.client_sockets = []

    def drop_client(self, sock):
        print "Dropping client on fd %d" % (sock.fileno())

        self.pwe.del_fd_event(sock)
        self.client_sockets.remove(sock)
        sock.close()
    
    def drop_source(self, sock):
        print "Dropping source on fd %d" % (sock.fileno())

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

    def export_new_stream(self, received, fd):

        try:
            coll_id, coll_name, stream_id, properties = received
        except ValueError:
            print >> sys.stderr, "Incorrect data format from source %d" % (fd)
            print >> sys.stderr, "Format should be (collection id, collection name, streamid, values dict)"
            return -1
        
        if not isinstance(properties, dict):
            print >> sys.stderr, "Values should expressed as a dictionary"
            return -1

        # XXX UNTESTED!!

        if coll_name not in self.collections.keys():
            return 0

        properties['stream_id'] = stream_id

        ns_data = pickle.dumps((coll_id, coll_name, False, [properties]))
        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_STREAMS, 
                len(ns_data))

        active = []
        for sock in self.collections[coll_name]:
            if sock not in self.client_sockets:
                continue
    
            try:
                sock.send(header + ns_data)
            except error, msg:
                print >> sys.stderr, "Error sending schemas to client fd %d: %s" % (sock.fileno(), msg[1])
                self.drop_client(sock)
            else:
                active.append(sock)
        self.collections[coll_name] = active

        return 0

    def export_live_data(self, received, fd):

        try:
            name, stream_id, timestamp, values = received
        except ValueError:
            print >> sys.stderr, "Incorrect data format from source %d" % (fd)
            print >> sys.stderr, "Format should be (name, streamid, timestamp, values dict)"
            return -1
        
        if not isinstance(values, dict):
            print >> sys.stderr, "Values should expressed as a dictionary"
            return -1
        
        if stream_id in self.subscribers.keys():
            active = []

            for sock, columns, start, end, col in self.subscribers[stream_id]:

                if timestamp < start:
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

                    try:
                        sock.send(header + contents)
                    except error, msg:
                        print >> sys.stderr, "Error sending live data to client fd %d: %s" % (sock.fileno(), msg[1])
                        self.drop_client(sock)
                    else:
                        if results != {}:
                            active.append((sock, columns, start, end, col))
            self.subscribers[stream_id] = active

        return 0

    def send_collections(self, sock, cols):

        col_pickle = pickle.dumps(cols)
        msglen = len(col_pickle)

        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_COLLECTIONS, msglen)

        try:
            sock.send(header + col_pickle)
        except error, msg:
            print >> sys.stderr, "Error sending collections to client fd %d" % (sock.fileno(), msg[1])
            self.drop_client(sock)
            return -1
        return 0

    def send_schema(self, sock, col_id):
        
        stream_schema, data_schema = self.db.get_collection_schema(col_id)

        # We just want the column names
        stream_schema = map(lambda a: a.name, stream_schema)
        data_schema = map(lambda a: a.name, data_schema)

        schema_pick = pickle.dumps((col_id, stream_schema, data_schema))

        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_SCHEMAS, len(schema_pick))

        try:
            sock.send(header + schema_pick)
        except error, msg:
            print >> sys.stderr, "Error sending schemas to client fd %d" % (sock.fileno(), msg[1])
            self.drop_client(sock)
            return -1
        return 0

    def send_streams(self, sock, col):
       
        if self.collections.has_key(col):
            if sock not in self.collections[col]:
                self.collections[col].append(sock)
        else:
            self.collections[col] = [sock]
       
        streams = self.db.select_streams_by_collection(col)
     
        i = 0
        while (i < len(streams)):

            start = i
            if len(streams) <= i + 100:
                end = len(streams)
                more = False
            else:
                end = i + 100
                more = True

            stream_data = pickle.dumps((col, more, streams[start:end]))
        
            header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_STREAMS, 
                    len(stream_data))
            try:
                sock.send(header + stream_data)
            except error, msg:
                print >> sys.stderr, "Error sending schemas to client fd %d: %s" % (sock.fileno(), msg[1])
                self.drop_client(sock)
                return -1

            i = end
        return 0
    
    def export_hist_block(self, sock, name, streamid, block, more):        

        contents = pickle.dumps((name, streamid, block, more))
        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_HISTORY, len(contents))

        try:
            sock.send(header + contents)
        except error, msg:
            print >> sys.stderr, "Error sending data to client fd %d: %s" % (sock.fileno(), msg[1])
            self.drop_client(sock)

            return -1

        return 0


    def send_full_history(self, streamid, sock, columns, start, end, name):
       
        hist = self.db.select_data(name, [streamid], columns, start, end)
        tosend = []
        c = 0

        for h in hist:
            
            tosend.append(h)
            c += 1

            if (c >= 200):
                if h != hist[-1]:
                    more = True
                else:
                    more = False

                if self.export_hist_block(sock, name, streamid, tosend, more) == -1:
                    return -1
                c = 0
                tosend = []

        if c != 0:
            return self.export_hist_block(sock, name, streamid, tosend, False)
        return 0
                    
    def subscribe(self, sock, submsg):
        name, start, end, cols, streams = pickle.loads(submsg)
        now = int(time.mktime(time.gmtime()))

        if start == 0 or start == None:
            start = now
        if end == 0:
            end = None
        for s in streams:

            # If live data is going to be required, add to the sub list
            if (end == None or end > now):
                if self.subscribers.has_key(s):
                    self.subscribers[s].append((sock, cols, start, end, name))
                else:
                    self.subscribers[s] = [(sock, cols, start, end, name)]

            # Send any historical data that we've been asked for
            if start < now:
                if self.send_full_history(s, sock, cols, start, end, name) == -1:
                    return -1

    def aggregate(self, sock, aggmsg):
        tup = pickle.loads(aggmsg)
        name, start, end, streams, aggcols, groupcols, binsize, funcname = tup




    def request_message(self, sock, reqmsg):

        req_hdr = struct.unpack(nntsc_req_fmt, 
                reqmsg[0:struct.calcsize(nntsc_req_fmt)])

        if req_hdr[0] == NNTSC_REQ_COLLECTION:
            # Requesting the collection list
            cols = self.db.list_collections()
            
            shrink = []
            for c in cols:
                shrink.append({"id":c['id'], "module":c['module'], "modsubtype":c['modsubtype']})

            return self.send_collections(sock, shrink)

        if req_hdr[0] == NNTSC_REQ_SCHEMA:
            col_id = pickle.loads(reqmsg[struct.calcsize(nntsc_req_fmt):])
            return self.send_schema(sock,col_id)


        if req_hdr[0] == NNTSC_REQ_STREAMS:
            col_id = pickle.loads(reqmsg[struct.calcsize(nntsc_req_fmt):])
            return self.send_streams(sock, col_id)

        return 0

    def client_message(self, sock, msg):

        error = 0
        header = struct.unpack(nntsc_hdr_fmt, msg[0:struct.calcsize(nntsc_hdr_fmt)])
        total_len = header[2] + struct.calcsize(nntsc_hdr_fmt)
        body = msg[struct.calcsize(nntsc_hdr_fmt):total_len]

        if len(msg) < total_len:
            return msg, 1, error

        if header[1] == NNTSC_REQUEST:
            if self.request_message(sock, body) == -1:
                error = 1

        if header[1] == NNTSC_AGGREGATE:
            if self.aggregate(sock, body) == -1:
                error = 1

        if header[1] == NNTSC_SUBSCRIBE:
            if self.subscribe(sock, body) == -1:
                error = 1

        return msg[total_len:], 0, error

    def receive_client(self, fd, evtype, sock, data):

        buf = data

        assert(evtype == 1)

        try:
            received = sock.recv(4096)
        except error, msg:
            print >> sys.stderr, "Error receiving data from client: %s" % (msg[1])
            self.drop_client(sock)
            return

        if len(received) == 0:
            # Client has disconnected
            self.drop_client(sock)
            return

        buf = buf + received
        error = 0

        while len(buf) > struct.calcsize(nntsc_hdr_fmt):
            buf, halt, error = self.client_message(sock, buf)

            if halt or error == -1:
                break
       
        if error == -1:
            self.pwe.update_fd_data(sock,buf)

    def receive_source(self, fd, evtype, sock, data):

        try:
            obj = sock.recv()
        except EOFError, msg:
            print >> sys.stderr, "Error receiving data from source %d: %s" % (fd, msg)
            self.drop_source(sock)
            return

        msgtype, contents = obj
        
        ret = 0

        if msgtype == 0:
            ret = self.export_live_data(contents, sock.fileno())
        if msgtype == 1:
            ret = self.export_new_stream(contents, sock.fileno())

        if ret < 0:
            self.drop_source(sock)


    def accept_connection(self, fd, evtype, sock, data):

        try:
            client, addr = sock.accept()
        except error, msg:
            print >> sys.stderr, "Error accepting connection: %s" % (msg[1])
            return

        print "Accepted connection on fd %d" % (client.fileno())

        self.client_sockets.append(client)
        self.pwe.add_fd_event(client, 1, "", self.receive_client)


    def register_source(self, pipe):
        print "Registering source on fd %d" % (pipe.fileno())
        
        self.pwe.add_fd_event(pipe, 1, "", self.receive_source)    

    def create_listener(self, port):
        
        try:
            s = socket(AF_INET, SOCK_STREAM)
        except error, msg:
            print >> sys.stderr, "Failed to create socket: %s" % (msg[1])
            return -1

        try:
            s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        except error, msg:
            print >> sys.stderr, "Failed to set SO_REUSEADDR: %s" % (msg[1])
            return -1


        try:
            s.bind(('', port))
        except error, msg:
            print >> sys.stderr, "Failed to bind to port %d: %s" % (port, msg[1])
            return -1

        try:
            s.listen(5)
        except error, msg:
            print >> sys.stderr, "Failed to listen on port %d: %s" % (port, msg[1])
            return -1

        return s

    def configure(self, conf_fname):
        nntsc_conf = load_nntsc_config(conf_fname)
        if nntsc_conf == 0:
            sys.exit(0)

        dbconf = get_nntsc_db_config(nntsc_conf)
        if dbconf == {}:
            sys.exit(1)

        self.db = Database(dbconf["name"], dbconf["user"], dbconf["pass"],
                dbconf["host"])
 
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

