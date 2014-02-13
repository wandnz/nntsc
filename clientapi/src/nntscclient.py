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


import sys,getopt,struct,pickle,time
from socket import *
from libnntscclient.protocol import *

class NNTSCClient:
    def __init__(self, sock):
        self.sock = sock
        self.buf = ""
        self.sentversion = False

    def disconnect(self):
        if self.sock != None:
            self.sock.close()
        self.sock = None

    def send_version_check(self):
        return 0

        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_VERSION_CHECK, 
                len(NNTSC_CLIENTAPI_VERSION))
            
        try:
            self.sock.sendall(header + NNTSC_CLIENTAPI_VERSION)
        except error, msg:
            print >> sys.stderr, "Error sending NNTSC_CLIENTAPI_VERSION: %s" % (msg[1])
            return -1

        self.sentversion = True
        return 0
        

    def send_request(self, reqtype, col, start=0):
        if self.sock == None:
            print >> sys.stderr, "Cannot send NNTSC_REQUEST on a closed socket!"
            return -1;

        if self.sentversion == False:
            if self.send_version_check() == -1:
                return -1 

        if reqtype == NNTSC_REQ_COLLECTION:
            col = 0

        request = struct.pack(nntsc_req_fmt, reqtype, col, start)

        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_REQUEST,
                struct.calcsize(nntsc_req_fmt))

        try:
            self.sock.sendall(header + request)
        except error, msg:
            print >> sys.stderr, "Error sending NNTSC_REQUEST %d for collection %d: %s" % (reqtype, col, msg[1])
            return -1

        return 0

    def subscribe_streams(self, name, columns, labels, start, end, aggs):
        if self.sock == None:
            print >> sys.stderr, "Cannot send NNTSC_SUBSCRIBE on a closed socket!"
            return -1;
        if self.sentversion == False:
            if self.send_version_check() == -1:
                return -1 

        # Our "labels" are actually a list of streams, which is how we used to
        # manage this sort of thing. Convert to the new label format for 
        # backwards compatibility   
        if type(labels) is list:
            labels = self.convert_streams_to_labels(labels)

        contents = pickle.dumps((name, start, end, columns, labels, aggs))
        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_SUBSCRIBE, len(contents))

        try:
            self.sock.sendall(header + contents)
        except error, msg:
            print >> sys.stderr, "Error sending NNTSC_SUBSCRIBE for %s: %s" % (name, msg[1])
            return -1

        return 0

    def request_aggregate(self, col, labels, start, end, aggcolumns, binsize,
            groupcolumns=[], aggfunc="avg"):

        if self.sock == None:
            print >> sys.stderr, "Cannot send NNTSC_AGGREGATE on a closed socket!"
            return -1;
        if self.sentversion == False:
            if self.send_version_check() == -1:
                return -1 
        
        # Our "labels" are actually a list of streams, which is how we used to
        # manage this sort of thing. Convert to the new label format for 
        # backwards compatibility   
        if type(labels) is list:
            labels = self.convert_streams_to_labels(labels)
        
        contents = pickle.dumps((col, start, end, labels, aggcolumns, 
                groupcolumns, binsize, aggfunc))
        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_AGGREGATE, len(contents))

        try:
            self.sock.sendall(header + contents)
        except error, msg:
            print >> sys.stderr, "Error sending NNTSC_AGGREGATE for %s: %s" % (col, msg[1])
            return -1

        return 0

    def request_percentiles(self, col, labels, start, end, binsize, 
            ntilecolumns, othercolumns=[], ntileaggfunc="avg", 
            otheraggfunc="avg"): 

        if self.sock == None:
            print >> sys.stderr, "Cannot send NNTSC_PERCENTILE on a closed socket!"
            return -1;
        
        if self.sentversion == False:
            if self.send_version_check() == -1:
                return -1 
        
        # Our "labels" are actually a list of streams, which is how we used to
        # manage this sort of thing. Convert to the new label format for 
        # backwards compatibility   
        if type(labels) is list:
            labels = self.convert_streams_to_labels(labels)
        
        contents = pickle.dumps((col, start, end, labels, binsize, 
                ntilecolumns, 
                othercolumns, ntileaggfunc, otheraggfunc))
        header = struct.pack(nntsc_hdr_fmt, 1, NNTSC_PERCENTILE, len(contents))

        try:
            self.sock.sendall(header + contents)
        except error, msg:
            print >> sys.stderr, "Error sending NNTSC_PERCENTILE for %s: %s" % (col, msg[1])
            return -1

        return 0

    def receive_message(self):
        if self.sock == None:
            print >> sys.stderr, "Cannot receive messages on a closed socket!"
            return -1;

        try:
            received = self.sock.recv(256000)
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

        if header[1] == NNTSC_VERSION_CHECK:
            version = pickle.loads(self.buf[header_end:total_len])
            if version != NNTSC_CLIENTAPI_VERSION:
                print >> sys.stderr, "Current NNTSC Client version %s does not match version required by server (%s)" % (NNTSC_CLIENTAPI_VERSION, version)
                print >> sys.stderr, "Closing client socket"
                self.disconnect()
                return -1, {}
            else:
                print >> sys.stderr, "Version check passed"

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

        if header[1] == NNTSC_ACTIVE_STREAMS:
            name, more, arrived = pickle.loads(self.buf[header_end:total_len])
            msgdict['collection'] = name
            msgdict['more'] = more
            msgdict['streams'] = arrived

        if header[1] == NNTSC_HISTORY:
            name, stream_id, data, more, binsize = pickle.loads(self.buf[header_end:total_len])
            msgdict['collection'] = name
            msgdict['streamid'] = stream_id
            msgdict['data'] = data
            msgdict['more'] = more
            msgdict['binsize'] = binsize

        if header[1] == NNTSC_LIVE:
            name, stream_id, data = pickle.loads(self.buf[header_end:total_len])
            msgdict['collection'] = name
            msgdict['streamid'] = stream_id
            msgdict['data'] = data

        if header[1] == NNTSC_PUSH:
            colid, timestamp = pickle.loads(self.buf[header_end:total_len])
            msgdict['collection'] = colid
            msgdict['timestamp'] = timestamp

        self.buf = self.buf[total_len:]
        return header[1], msgdict

    def convert_streams_to_labels(self, streams):

        labels = {}

        for s in streams:
            # XXX Make the labels strings, otherwise we run into casting
            # issues later on with Brendon's hax ampy code. 
            labels[str(s)] = [s]
        return labels

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
