#
# This file is part of NNTSC.
#
# Copyright (C) 2013-2017 The University of Waikato, Hamilton, New Zealand.
#
# Authors: Shane Alcock
#          Brendon Jones
#
# All rights reserved.
#
# This code has been developed by the WAND Network Research Group at the
# University of Waikato. For further information please see
# http://www.wand.net.nz/
#
# NNTSC is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.
#
# NNTSC is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with NNTSC; if not, write to the Free Software Foundation, Inc.
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#
# Please report any bugs, questions or comments to contact@wand.net.nz
#

from libnntsc.parsers.common import NNTSCParser
import libnntscclient.logger as logger

class RRDMuninbytesParser(NNTSCParser):

    def __init__(self, db, influxdb=None):
        super(RRDMuninbytesParser, self).__init__(db, influxdb)

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
