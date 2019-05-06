#
# This file is part of NNTSC.
#
# Copyright (C) 2013-2019 The University of Waikato, Hamilton, New Zealand.
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
from libnntsc.dberrorcodes import DB_DATA_ERROR
import libnntscclient.logger as logger

class AmpExternalParser(NNTSCParser):
    def __init__(self, db, influxdb=None):
        super(AmpExternalParser, self).__init__(db, influxdb)

        self.influxdb = influxdb

        self.streamtable = "streams_amp_external"
        self.datatable = "data_amp_external"
        self.colname = "amp_external"
        self.source = "amp"
        self.module = "external"

        self.streamcolumns = [
            {"name":"source", "type":"varchar", "null":False},
            {"name":"destination", "type":"varchar", "null":False},
            {"name":"command", "type":"varchar", "null":False},
        ]

        self.uniquecolumns = ['source', 'destination', 'command']

        self.streamindexes = [
            {"name": "", "columns": ['source']},
            {"name": "", "columns": ['destination']},
            {"name": "", "columns": ['command']},
        ]

        self.datacolumns = [
            {"name":"value", "type":"integer", "null":True},
        ]

        self.dataindexes = [
        ]

        self.matrix_cq = [
            ("value", "mean", "value_avg"),
            ("value", "stddev", "value_stddev"),
        ]

    # convert stream properties into a key
    def _stream_key(self, stream_data):
        key = ()
        for column in self.uniquecolumns:
            value = stream_data[column]
            key += (value,)
        return key

    # convert raw data into stream properties
    def _stream_properties(self, data):
        props = {}
        for column in self.uniquecolumns:
            props[column] = data[column]
        return props

    def create_existing_stream(self, stream_data):
        """Extract the stream key from the stream data provided by NNTSC
           when the AMP module is first instantiated"""

        key = self._stream_key(stream_data)
        self.streams[key] = stream_data["stream_id"]

    def process_data(self, timestamp, data, source):
        for result in data['results']:
            result['source'] = source
            result['command'] = data['command']
            # TODO should we use a string like "none" instead? Or the command?
            # if the test had no destination, use the source as the destination
            if result['destination'] is None:
                result['destination'] = result['source']

            key = self._stream_key(self._stream_properties(result))

            if key in self.streams:
                stream_id = self.streams[key]
            else:
                stream_id = self.create_new_stream(
                        self._stream_properties(result),
                        timestamp, not self.have_influx)
                if stream_id < 0:
                    logger.log("AMPModule: Cannot create stream for: ")
                    logger.log("AMPModule: external %s %s\n", source,
                            result['destination'])
                    return
                self.streams[key] = stream_id

            self.insert_data(stream_id, timestamp, result)
            self.db.update_timestamp(self.datatable, [stream_id], timestamp,
                    self.have_influx)

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
