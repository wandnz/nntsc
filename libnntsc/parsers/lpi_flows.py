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
from libnntsc.dberrorcodes import *
from libnntsc.parsers.lpi_bytes import LPIBytesParser

# We can only really borrow process_data from the LPI bytes parser but
# that's better than nothing...
class LPIFlowsParser(LPIBytesParser):
    def __init__(self, db):
        super(LPIFlowsParser, self).__init__(db)

        self.streamtable = "streams_lpi_flows"
        self.datatable = "data_lpi_flows"
        self.colname = "lpi_flows"
        self.source = "lpi"
        self.module = "flows"

        self.streamcolumns = [
            {"name":"source", "type":"varchar", "null":False},
            {"name":"user", "type":"varchar", "null":False},
            {"name":"dir", "type":"varchar", "null":False},
            {"name":"freq", "type":"integer", "null":False},
            {"name":"protocol", "type":"varchar", "null":False},
            {"name":"metric", "type":"varchar", "null":False},
        ]
        self.uniquecolumns = ['source', 'user', 'dir', 'freq', 'protocol',
                'metric']
        self.streamindexes = []
        self.datacolumns = [
            {"name":"flows", "type":"bigint"}
        ]
        self.dataindexes = []

    def create_existing_stream(self, stream_data):

        key = (stream_data['source'], stream_data['user'], \
                stream_data['dir'], stream_data['freq'], \
                stream_data['protocol'], stream_data['metric'])

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
        if 'metric' not in data:
            logger.log("Error: no metric specified in %s result" % \
                    (self.colname))
            return None, None


        props['source'] = data['id']
        props['user'] = data['user']
        props['dir'] = data['dir']
        props['freq'] = data['freq']
        props['protocol'] = protocol

        if data['metric'] == "newflows":
            props['metric'] = "new"
        elif data['metric'] == "peakflows":
            props['metric'] = "peak"
        else:
            logger.log("Error: unknown metric for %s (%s)" % (self.colname, \
                    data['metric']))
            return None, None

        key = (props['source'], props['user'], props['dir'], props['freq'],
                props['protocol'], props['metric'])
        return props, key

    def _result_dict(self, val):
        return {'flows':val}

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
