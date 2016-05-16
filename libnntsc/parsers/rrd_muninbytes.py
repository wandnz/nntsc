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

from libnntsc.parsers.common import NNTSCParser
import libnntscclient.logger as logger

class RRDMuninbytesParser(NNTSCParser):

    def __init__(self, db):
        super(RRDMuninbytesParser, self).__init__(db)

        self.streamtable = "streams_rrd_muninbytes"
        self.datatable = "data_rrd_muninbytes"
        self.colname = "rrd_muninbytes"
        self.source = "rrd"
        self.module = "muninbytes"
        
        self.streamcolumns = [ 
            {"name":"filename", "type":"varchar", "null":False},
            {"name":"switch", "type":"varchar", "null":False},
            {"name":"interface", "type":"varchar", "null":False},
            {"name":"interfacelabel", "type":"varchar"},
            {"name":"direction", "type":"varchar", "null":False},
            {"name":"minres", "type":"integer", "null":False, "default":"300"},
            {"name":"highrows", "type":"integer", "null":False, 
                    "default":"1008"}
        ]

        self.uniquecolumns = ['filename', 'interface', 'switch', 'direction']
        self.streamindexes = []
        self.datacolumns = [
            {"name":"bytes", "type":"bigint"}
        ]
        self.dataindexes = []

    def insert_stream(self, streamparams):
        if 'switch' not in streamparams:
            logger.log("Missing 'switch' parameter for Muninbytes RRD")
            return DB_DATA_ERROR
        if 'interface' not in streamparams:
            logger.log("Missing 'interface' parameter for Muninbytes RRD")
            return DB_DATA_ERROR
        if 'direction' not in streamparams:
            logger.log("Missing 'direction' parameter for Muninbytes RRD")
            return DB_DATA_ERROR

        if streamparams['direction'] not in ["sent", "received"]:
            logger.log("'direction' must be either 'sent' or 'received', not %s" % (streamparams['direction']))
            return DB_DATA_ERROR

        streamparams['filename'] = streamparams.pop('file')
        
        if 'interfacelabel' not in streamparams:
            streamparams['interfacelabel'] = None
        
        return self.create_new_stream(streamparams, 0)

    def process_data(self, stream, ts, line):
        assert(len(line) == 1)

        exportdict = {}

        line_map = {0:"bytes"}

        for i in range(0, len(line)):
            if line[i] == None:
                val = None
            else:
                val = int(line[i])

            exportdict[line_map[i]] = val

        self.insert_data(stream, ts, exportdict)

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
