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


import libnntscclient.logger as logger
import sys, string
from libnntsc.dberrorcodes import *
from libnntsc.parsers.common import NNTSCParser

class LPIBytesParser(NNTSCParser):

    def __init__(self, db):
        super(LPIBytesParser, self).__init__(db)

        self.streamtable = "streams_lpi_bytes"
        self.datatable = "data_lpi_bytes"
        self.colname = "lpi_bytes"
        self.source = "lpi"
        self.module = "bytes"

        self.streamcolumns = [
            {"name":"source", "type":"varchar", "null":False},
            {"name":"user", "type":"varchar", "null":False},
            {"name":"dir", "type":"varchar", "null":False},
            {"name":"freq", "type":"integer", "null":False},
            {"name":"protocol", "type":"varchar", "null":False},
        ]

        self.uniquecolumns = ['source', 'user', 'dir', 'freq', 'protocol']
        self.streamindexes = []
        self.datacolumns = [
            {"name":"bytes", "type":"bigint"}
        ]
        self.dataindexes = []


    def create_existing_stream(self, stream_data):
        key = (stream_data['source'], stream_data['user'], stream_data['dir'], \
                stream_data['freq'], stream_data['protocol'])

        self.streams[key] = stream_data['stream_id']

    def _stream_properties(self, protocol, data):
        props = {}

        if 'id' not in data:
            logger.log("Error: no source specified in %s result" % \
                    (self.colname))
            return None, None
        if 'user' not in data:
            logger.log("Error: no user specified in %s result" % \
                    (self.colname))
            return None, None
        if 'dir' not in data:
            logger.log("Error: no direction specified in %s result" % \
                    (self.colname))
            return None, None
        if 'freq' not in data:
            logger.log("Error: no frequency specified in %s result" % \
                    (self.colname))
            return None, None

        props['source'] = data['id']
        props['user'] = data['user']
        props['dir'] = data['dir']
        props['freq'] = data['freq']
        props['protocol'] = protocol

        key = (props['source'], props['user'], props['dir'], props['freq'],
                props['protocol'])
        return props, key

    def _result_dict(self, val):
        return {'bytes':val}

    def process_data(self, protomap, data):
        done = []

        for p, val in data['results'].items():
            if p not in protomap:
                logger.log("LPI Bytes: Unknown protocol id: %u" % (p))
                return DB_DATA_ERROR

            streamparams, key = self._stream_properties(protomap[p], data)

            if key is None:
                logger.log("Failed to determine stream for %s result" %
                        (self.colname))
                return DB_DATA_ERROR

            if key not in self.streams:
                if val == 0:
                    continue
                streamid = self.create_new_stream(streamparams, data['ts'])
                if streamid < 0:
                    logger.log("Failed to create new %s stream" % \
                            (self.colname))
                    logger.log("%s" % (str(streamparams)))
                    return
                self.streams[key] = streamid
            else:
                streamid = self.streams[key]

            self.insert_data(streamid, data['ts'], self._result_dict(val))
            done.append(streamid)

        self.db.update_timestamp(self.datatable, done, data['ts'])

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
