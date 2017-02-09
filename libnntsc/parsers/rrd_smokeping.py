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

class RRDSmokepingParser(NNTSCParser):

    def __init__(self, db):
        super(RRDSmokepingParser, self).__init__(db)

        self.streamtable = "streams_rrd_smokeping"
        self.datatable = "data_rrd_smokeping"
        self.colname = "rrd_smokeping"
        self.source = "rrd"
        self.module = "smokeping"

        self.streamcolumns = [
            {"name":"filename", "type":"varchar", "null":False},
            {"name":"source", "type":"varchar", "null":False},
            {"name":"host", "type":"varchar", "null":False},
            {"name":"minres", "type":"integer", "null":False, "default":"300"},
            {"name":"highrows", "type":"integer", "null":False,
                    "default":"1008"}
        ]

        self.uniquecolumns = ['filename', 'source', 'host']
        self.streamindexes = []

        self.datacolumns = [
            {"name":"loss", "type":"smallint"},
            {"name":"median", "type":"double precision"},
            {"name":"pings", "type":"double precision[]"}
        ]
        self.dataindexes = []

    def insert_stream(self, streamparams):
        if 'source' not in streamparams:
            logger.log("Missing 'source' parameter for Smokeping RRD")
            return DB_DATA_ERROR
        if 'host' not in streamparams:
            logger.log("Missing 'host' parameter for Smokeping RRD")
            return DB_DATA_ERROR
        if 'name' not in streamparams:
            logger.log("Missing 'name' parameter for Smokeping RRD")
            return DB_DATA_ERROR

        streamparams['filename'] = streamparams.pop('file')

        return self.create_new_stream(streamparams, 0)


    def process_data(self, stream, ts, line):
        kwargs = {}

        if len(line) >= 1:
            if line[1] == None:
                kwargs['loss'] = None
            else:
                kwargs['loss'] = int(float(line[1]))

        if len(line) >= 2:
            if line[2] == None:
                kwargs['median'] = None
            else:
                kwargs['median'] = round(float(line[2]) * 1000.0, 6)

        kwargs['pings'] = []

        for i in range(3, len(line)):
            if line[i] == None:
                val = None
            else:
                val = round(float(line[i]) * 1000.0, 6)

            kwargs['pings'].append(val)

        casts = {"pings":"double precision[]"}
        self.insert_data(stream, ts, kwargs, casts)


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
