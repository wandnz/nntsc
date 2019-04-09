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
from libnntsc.dberrorcodes import DB_DATA_ERROR
import libnntscclient.logger as logger

class AmpFastpingParser(NNTSCParser):
    def __init__(self, db, influxdb=None):
        super(AmpFastpingParser, self).__init__(db, influxdb)

        self.influxdb = influxdb

        self.streamtable = "streams_amp_fastping"
        self.datatable = "data_amp_fastping"
        self.colname = "amp_fastping"
        self.source = "amp"
        self.module = "fastping"

        self.streamcolumns = [
            {"name":"source", "type":"varchar", "null":False},
            {"name":"destination", "type":"varchar", "null":False},
            {"name":"family", "type":"varchar", "null":False},
            {"name":"packet_size", "type":"integer", "null":False},
            {"name":"packet_rate", "type":"bigint", "null":False},
            {"name":"packet_count", "type":"bigint", "null":False},
            {"name":"preprobe", "type":"boolean", "null":False},
        ]

        self.uniquecolumns = ['source', 'destination', 'family', 'packet_size',
                'packet_rate', 'packet_count', 'preprobe']

        self.streamindexes = [
            {"name": "", "columns": ['source']},
            {"name": "", "columns": ['destination']}
        ]

        self.datacolumns = [
            {"name":"median", "type":"integer", "null":True},
            {"name":"percentiles", "type":"integer[]", "null":True},
            {"name":"lossrate", "type":"float", "null":False},
        ]

        self.dataindexes = [
        ]

        self.matrix_cq = [
            ("median", "mean", "median_avg"),
            ("median", "stddev", "median_stddev"),
            ("lossrate", "mean", "lossrate_avg"),
            ("lossrate", "stddev", "lossrate_stddev"),
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
            if column == "family":
                if '.' in data['address']:
                    value = "ipv4"
                else:
                    value = "ipv6"
            else:
                value = data[column]
            props[column] = value
        return props

    def create_existing_stream(self, stream_data):
        """Extract the stream key from the stream data provided by NNTSC
           when the AMP module is first instantiated"""

        key = self._stream_key(stream_data)
        self.streams[key] = stream_data["stream_id"]

    def _mangle_result(self, data):
        # Our columns are slightly different to the names that AMPsave uses,
        # so we'll have to mangle them to match what we're expecting
        key = self._stream_key(self._stream_properties(data))

        mangled = {}
        mangled['source'] = key[0]
        mangled['destination'] = key[1]
        mangled['family'] = key[2]

        rtt = data['results'][0]['rtt']
        mangled['median'] = int(rtt['percentiles'][9])
        mangled['lossrate'] = 1.0 - (rtt['samples'] / float(key[5]))
        mangled['percentiles'] = rtt['percentiles']

        return mangled, key

    def process_data(self, timestamp, data, source):
        data['source'] = source

        mangled, key = self._mangle_result(data)

        if key in self.streams:
            stream_id = self.streams[key]
        else:
            stream_id = self.create_new_stream(self._stream_properties(data),
                    timestamp, not self.have_influx)
            if stream_id < 0:
                logger.log("AMPModule: Cannot create stream for: ")
                logger.log("AMPModule: fastping %s %s\n", source,
                        mangled['destination'])
                return
            self.streams[key] = stream_id

        if self.influxdb:
            casts = {"percentiles":str}
        else:
            casts = {"percentiles":"integer[]"}
        self.insert_data(stream_id, timestamp, mangled, casts)
        self.db.update_timestamp(self.datatable, [stream_id], timestamp,
                self.have_influx)

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
