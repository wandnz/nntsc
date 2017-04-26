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

from libnntsc.dberrorcodes import *
from libnntsc.parsers.common import NNTSCParser
import libnntscclient.logger as logger

class AmpThroughputParser(NNTSCParser):
    def __init__(self, db, influxdb=None):
        super(AmpThroughputParser, self).__init__(db, influxdb)

        self.streamtable = "streams_amp_throughput"
        self.datatable = "data_amp_throughput"
        self.colname = "amp_throughput"
        self.source = "amp"
        self.module = "throughput"

        self.streamcolumns = [
            {"name":"source", "type":"varchar", "null":False},
            {"name":"destination", "type":"varchar", "null":False},
            {"name":"direction", "type":"varchar", "null":False},
            {"name":"address", "type":"inet", "null":False},
            {"name":"duration", "type":"integer", "null":False},
            {"name":"writesize", "type":"integer", "null":False},
            {"name":"tcpreused", "type":"boolean", "null":False},
            {"name":"protocol", "type":"varchar", "null":False},
        ]

        self.uniquecolumns = [
            'source', 'destination', 'direction', 'address',
            'duration', 'writesize', 'tcpreused', 'protocol',
        ]

        self.streamindexes = [
            {"name": "", "columns": ['source']},
            {"name": "", "columns": ['destination']}
        ]

        self.datacolumns = [
            {"name":"bytes", "type":"bigint", "null":True},
            {"name":"packets", "type":"bigint", "null":True},
            {"name":"rate", "type":"float", "null":True},
            {"name":"runtime", "type":"integer", "null":True}
        ]

        self.matrix_cq = [
            ('bytes', 'sum', 'bytes'),
            ('packets', 'sum', 'packets'),
            ('runtime', 'sum', 'runtime'),
            ('rate', 'stddev', 'rate'),
        ]

    def _construct_key(self, stream_data):
        src = str(stream_data["source"])
        dest = str(stream_data["destination"])
        direction = str(stream_data["direction"])
        remote = stream_data["address"]
        duration = str(stream_data["duration"])
        writesize = str(stream_data["writesize"])
        reused = stream_data["tcpreused"]
        protocol = str(stream_data["protocol"])

        key = (src, dest, direction, remote, duration, writesize, reused,
                protocol)
        return key

    def create_existing_stream(self, stream_data):
        key = self._construct_key(stream_data)
        streamid = stream_data["stream_id"]
        self.streams[key] = streamid


    def _process_single_result(self, timestamp, resdict):
        key = self._construct_key(resdict)

        if key in self.streams:
            stream_id = self.streams[key]
        else:
            stream_id = self.create_new_stream(resdict, timestamp,
                    not self.have_influx)

            if stream_id < 0:
                logger.log("AMPModule: Cannot create new throughput stream")
                logger.log("AMPModule: %s:%s:%s:%s" % ( \
                        resdict['source'], resdict['destination'],
                        resdict['duration'],
                        resdict['writesize']))
                return stream_id
            else:
                self.streams[key] = stream_id

        self.insert_data(stream_id, timestamp, resdict)
        return stream_id


    def process_data(self, timestamp, data, source):
        done = {}
        for result in data['results']:
            resdict = {}
            resdict['source'] = source
            resdict['destination'] = data['target']
            resdict['address'] = data['address']
            resdict['direction']  = result['direction']
            resdict['duration'] = result['duration']
            resdict['runtime'] = result['runtime']
            resdict['bytes'] = result['bytes']
            resdict['protocol'] = data['protocol']

            if result['duration'] is not None and result['duration'] > 0:
                resdict['rate'] = result['bytes'] / float(result['duration'])
            else:
                resdict['rate'] = None

            # new style has write_size fixed for all schedule items in a test,
            # but we should try to be backwards compatible for a while at least.
            if 'write_size' in result:
                resdict['writesize'] = result['write_size']
            else:
                resdict['writesize'] = data['write_size']

            if 'packets' in result:
                resdict['packets'] = result['packets']
            else:
                resdict['packets'] = None
            resdict['tcpreused'] = result['tcpreused']

            streamid = self._process_single_result(timestamp, resdict)
            if streamid < 0:
                return
            done[streamid] = 0

        self.db.update_timestamp(self.datatable, done.keys(), timestamp,
                self.have_influx)

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
