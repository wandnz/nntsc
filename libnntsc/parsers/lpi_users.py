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
import sys, string
from libnntsc.parsers.lpi_bytes import LPIBytesParser

# We can only inherit process_data from the LPIBytesParser but that's
# better than nothing.

class LPIUsersParser(LPIBytesParser):
    def __init__(self, db):
        super(LPIUsersParser, self).__init__(db)

        self.streamtable = "streams_lpi_users"
        self.datatable = "data_lpi_users"
        self.colname = "lpi_users"
        self.module = "users"
        self.source = "lpi"

        self.streamcolumns = [ \
            {"name":"source", "type":"varchar", "null":False},
            {"name":"metric", "type":"varchar", "null":False},
            {"name":"freq", "type":"integer", "null":False},
            {"name":"protocol", "type":"varchar", "null":False},
        ]
        self.uniquecolumns = ['source', 'metric', 'freq', 'protocol']
        self.datacolumns = [
            {"name":"users", "type":"integer"}
        ]


    def create_existing_stream(self, stream_data):
        key = (stream_data['source'], stream_data['freq'], \
                stream_data['protocol'], stream_data['metric'])

        self.streams[key] = stream_data['stream_id']

    def _stream_properties(self, protocol, data):
        props = {}

        if 'id' not in data:
            logger.log("Error: no source specified in %s result" % \
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

        if data['metric'] == "activeusers":
            props['metric'] = "active"
        elif data['metric'] == "observedusers":
            props['metric']  = "observed"
        else:
            logger.log("Error: invalid metric for %s (%s))" % (self.colname, \
                    data['metric']))
            return None, None

        props['source'] = data['id']
        props['freq'] = data['freq']
        props['protocol'] = protocol

        key = (props['source'], props['freq'], props['protocol'],
                props['metric'])
        return props, key

    def _result_dict(self, val):
        return {'users':val}

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
