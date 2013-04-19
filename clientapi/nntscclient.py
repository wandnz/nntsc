#!/usr/bin/env python

import sys,getopt,struct,pickle,time
from socket import *
from libnntsc.export import *

class NNTSCClient:
    def __init__(self, sock):
        self.sock = sock
        self.buf = ""

    def disconnect(self):
        self.sock.close()
        self.sock = None

    def send_request(self, reqtype, col):
        if self.sock == None:
            print >> sys.stderr, "Cannot send NNTSC_REQUEST on a closed socket!"
            return -1;
        
        request = struct.pack(nntsc_req_fmt, reqtype)
        if reqtype == NNTSC_REQ_COLLECTION:
            col = -1

        col_pick = pickle.dumps(col)

        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_REQUEST,
                struct.calcsize(nntsc_req_fmt) + len(col_pick))

        try:
            self.sock.sendall(header + request + col_pick)
        except error, msg:
            print >> sys.stderr, "Error sending NNTSC_REQUEST %d for collection %d: %s" % (reqtype, col, msg[1])
            return -1

        return 0
 
    def subscribe_streams(self, name, columns, streams, start, end):
        if self.sock == None:
            print >> sys.stderr, "Cannot send NNTSC_SUBSCRIBE on a closed socket!"
            return -1;

        contents = pickle.dumps((name, start, end, columns, streams))
        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_SUBSCRIBE, len(contents))

        try:
            self.sock.sendall(header + contents)
        except error, msg:
            print >> sys.stderr, "Error sending NNTSC_SUBSCRIBE for %s: %s" % (name, msg[1])
            return -1

        return 0
    
    def request_aggregate(self, col, streams, start, end, aggcolumns, binsize,
            groupcolumns=[], aggfunc="avg"):
        
        if self.sock == None:
            print >> sys.stderr, "Cannot send NNTSC_AGGREGATE on a closed socket!"
            return -1;
        contents = pickle.dumps((col, start, end, streams, aggcolumns, groupcolumns, 
                binsize, aggfunc))
        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_AGGREGATE, len(contents))

        try:
            self.sock.sendall(header + contents)
        except error, msg:
            print >> sys.stderr, "Error sending NNTSC_AGGREGATE for %s: %s" % (col, msg[1])
            return -1

        return 0

 
    def receive_message(self):
        if self.sock == None:
            print >> sys.stderr, "Cannot receive messages on a closed socket!"
            return -1;

        try:
            received = self.sock.recv(4096)
        except error, msg:
            print >> sys.stderr, "Error receiving data from client: %s" % (msg[1])
            return -1
        
        if len(received) == 0:
            return 0; 

        self.buf += received
        return len(received)

    def parse_message(self):

        if len(self.buf) < struct.calcsize(nntsc_hdr_fmt):
            return -1, {}

        header_end = struct.calcsize(nntsc_hdr_fmt)
        header = struct.unpack(nntsc_hdr_fmt, self.buf[0:header_end])

        total_len = header[2] + header_end

        if len(self.buf) < total_len:
            return -1, {}

        msgdict = {}

        if header[1] == NNTSC_COLLECTIONS:
            col_list = pickle.loads(self.buf[header_end:total_len])
            msgdict['collections'] = col_list

        if header[1] == NNTSC_SCHEMAS:
            name, ss, ds = pickle.loads(self.buf[header_end:total_len])
            msgdict['collection'] = name
            msgdict['streamschema'] = ss
            msgdict['dataschema'] = ds

        if header[1] == NNTSC_STREAMS:
            name, more, arrived = pickle.loads(self.buf[header_end:total_len])
            msgdict['collection'] = name
            msgdict['more'] = more
            msgdict['streams'] = arrived

        if header[1] == NNTSC_HISTORY:
            name, stream_id, data, more, binsize, agg = pickle.loads(self.buf[header_end:total_len])
            msgdict['collection'] = name
            msgdict['streamid'] = stream_id
            msgdict['data'] = data
            msgdict['more'] = more
            msgdict['binsize'] = binsize
            msgdict['aggregator'] = agg

        if header[1] == NNTSC_LIVE:
            name, stream_id, data = pickle.loads(self.buf[header_end:total_len])
            msgdict['collection'] = name
            msgdict['streamid'] = stream_id
            msgdict['data'] = data

        self.buf = self.buf[total_len:]
        return header[1], msgdict
            


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
