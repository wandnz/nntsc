#
# This file is part of NNTSC.
#
# Copyright (C) 2013-2020 The University of Waikato, Hamilton, New Zealand.
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

class AmpSipParser(NNTSCParser):

    def __init__(self, db, influxdb=None):
        super(AmpSipParser, self).__init__(db, influxdb)

        self.streamtable = "streams_amp_sip"
        self.datatable = "data_amp_sip"
        self.colname = "amp_sip"
        self.source = "amp"
        self.module = "sip"

        # XXX address or family?
        self.streamcolumns = [
            {"name": "source", "type": "varchar", "null": False},
            {"name": "destination", "type": "varchar", "null": False},
            {"name": "proxy", "type": "varchar", "null": False},
            {"name": "address", "type": "inet", "null": False},
            {"name": "direction", "type": "varchar", "null": False},
            {"name": "filename", "type": "varchar", "null": False},
            {"name": "repeat", "type": "boolean", "null": False},
            {"name": "max_duration", "type": "integer", "null": False},
            {"name": "dscp", "type": "varchar", "null": False},
        ]

        self.uniquecolumns = [
            'source', 'destination', 'proxy', 'address', 'direction',
            'filename', 'repeat', 'max_duration', 'dscp'
        ]

        self.streamindexes = [
            {"name": "", "columns": ['source']},
            {"name": "", "columns": ['destination']},
        ]

        # XXX what happens if sip test can't resolve a target?
        # TODO rx/tx jitter and loss summaries?
        self.datacolumns = [
            {"name": "response_time", "type": "integer", "null": True},
            {"name": "connect_time", "type": "integer", "null": True},
            {"name": "duration", "type": "integer", "null": True},
            {"name": "rtt_max", "type": "integer", "null": True},
            {"name": "rtt_min", "type": "integer", "null": True},
            {"name": "rtt_mean", "type": "integer", "null": True},
            {"name": "rtt_sd", "type": "integer", "null": True},
            {"name": "packets", "type": "integer", "null": True},
            {"name": "bytes", "type": "integer", "null": True},
            {"name": "lost", "type": "integer", "null": True},
            {"name": "discarded", "type": "integer", "null": True},
            {"name": "reordered", "type": "integer", "null": True},
            {"name": "duplicated", "type": "integer", "null": True},
            {"name": "mos", "type": "float", "null": True},
#            {"name": "rx_packets", "type": "integer", "null": True},
#            {"name": "rx_bytes", "type": "integer", "null": True},
#            {"name": "rx_lost", "type": "integer", "null": True},
#            {"name": "rx_discarded", "type": "integer", "null": True},
#            {"name": "rx_reordered", "type": "integer", "null": True},
#            {"name": "rx_duplicated", "type": "integer", "null": True},
#            {"name": "rx_mos", "type": "float", "null": True},
#            {"name": "tx_packets", "type": "integer", "null": True},
#            {"name": "tx_bytes", "type": "integer", "null": True},
#            {"name": "tx_lost", "type": "integer", "null": True},
#            {"name": "tx_discarded", "type": "integer", "null": True},
#            {"name": "tx_reordered", "type": "integer", "null": True},
#            {"name": "tx_duplicated", "type": "integer", "null": True},
#            {"name": "tx_mos", "type": "float", "null": True},
            # influx can't handle every column being null, so add one that
            # is always true
            {"name": "unused", "type": "boolean", "null": False},
        ]

        self.matrix_cq = [
            ('response_time', 'mean', 'response_time_avg'),
            ('response_time', 'stddev', 'response_time_stddev'),
            ('connect_time', 'mean', 'connect_time_avg'),
            ('connect_time', 'stddev', 'connect_time_stddev'),
            ('mos', 'mean', 'mos_avg'),
            ('mos', 'stddev', 'mos_stddev'),
            ('rtt_mean', 'mean', 'rtt_mean_avg'),
            ('rtt_mean', 'stddev', 'rtt_mean_stddev'),
        ]


    def create_existing_stream(self, stream_data):
        key = self._construct_key(stream_data)
        streamid = stream_data["stream_id"]
        self.streams[key] = streamid


    def _construct_key(self, streamdata):
        src = str(streamdata["source"])
        dest = str(streamdata["destination"])
        proxy = str(streamdata["proxy"])
        address = streamdata["address"]
        direction = streamdata["direction"]
        filename = streamdata["filename"]
        repeat = str(streamdata["repeat"])
        max_duration = str(streamdata["max_duration"])
        dscp = str(streamdata["dscp"])

        return (src, dest, proxy, address, direction, filename, repeat,
                max_duration, dscp)


    def _process_single_result(self, timestamp, resdict):
        key = self._construct_key(resdict)

        if key in self.streams:
            stream_id = self.streams[key]
        else:
            stream_id = self.create_new_stream(resdict, timestamp,
                    not self.have_influx)

            if stream_id < 0:
                logger.log("AMPModule: Cannot create new SIP stream")
                logger.log("AMPModule: %s:%s:%s" % (
                        resdict['source'], resdict['destination']))
                return stream_id
            else:
                self.streams[key] = stream_id
        self.insert_data(stream_id, timestamp, resdict)
        return stream_id


    def process_data(self, timestamp, data, source):
        done = {}

        # fake two different result blocks, one for each direction
        for result in data['results']:
            for direction in ['rx', 'tx']:
                resdict = {}
                resdict['source'] = source
                resdict['destination'] = data['uri']
                resdict['proxy'] = ",".join(data['proxy'])
                resdict['address'] = data['address']
                resdict['direction'] = direction
                resdict['filename'] = data['filename']
                resdict['repeat'] = data['repeat']
                resdict['max_duration'] = data['max_duration']
                resdict['dscp'] = data['dscp']
                resdict['unused'] = True

                resdict['response_time'] = result['time_till_first_response']
                resdict['connect_time'] = result['time_till_connected']
                resdict['duration'] = result['duration']

                if 'rtt' in result and result['rtt'] is not None:
                    resdict['rtt_max'] = result['rtt']['maximum']
                    resdict['rtt_min'] = result['rtt']['minimum']
                    resdict['rtt_mean'] = result['rtt']['mean']
                    resdict['rtt_sd'] = result['rtt']['sd']

                if direction in result and result[direction] is not None:
                    resdict['packets'] = result[direction]['packets']
                    resdict['bytes'] = result[direction]['bytes']
                    resdict['lost'] = result[direction]['lost']
                    resdict['discarded'] = result[direction]['discarded']
                    resdict['reordered'] = result[direction]['reordered']
                    resdict['duplicated'] = result[direction]['duplicated']
                    resdict['mos'] = result[direction]['mos']['itu_mos']

                streamid = self._process_single_result(timestamp, resdict)
                if streamid < 0:
                    return
                done[streamid] = 0

        self.db.update_timestamp(self.datatable, list(done.keys()), timestamp,
                self.have_influx)

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
