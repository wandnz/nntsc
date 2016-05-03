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


from libnntsc.dberrorcodes import *
from libnntsc.parsers.common import NNTSCParser
import libnntscclient.logger as logger

class AmpUdpstreamParser(NNTSCParser):

    def __init__(self, db, influxdb=None):
        super(AmpUdpstreamParser, self).__init__(db, influxdb)

        self.streamtable = "streams_amp_udpstream"
        self.datatable = "data_amp_udpstream"
        self.colname = "amp_udpstream"
        self.source = "amp"
        self.module = "udpstream"

        self.streamcolumns = [
            {"name":"source", "type":"varchar", "null":False},
            {"name":"destination", "type":"varchar", "null":False},
            {"name":"address", "type":"inet", "null":False},
            {"name":"direction", "type":"varchar", "null":False},
            {"name":"packet_size", "type":"smallint", "null":False},
            {"name":"packet_spacing", "type":"integer", "null":False},
            {"name":"packet_count", "type": "integer", "null":False},
            {"name":"dscp", "type":"smallint", "null": False},
        ]

        self.uniquecolumns = [
            'source', 'destination', 'address', 'direction', 'packet_size', \
            'packet_spacing', 'packet_count', 'dscp'
        ]

        self.streamindexes = [
            {"name": "", "columns": ['source']},
            {"name": "", "columns": ['destination']}
        ]

        self.datacolumns = [
            {"name": "mean_rtt", "type": "integer", "null": True},
            {"name": "mean_jitter", "type": "integer", "null": True},
            {"name": "jitter_percentile_10", "type": "integer", "null": True},
            {"name": "jitter_percentile_20", "type": "integer", "null": True},
            {"name": "jitter_percentile_30", "type": "integer", "null": True},
            {"name": "jitter_percentile_40", "type": "integer", "null": True},
            {"name": "jitter_percentile_50", "type": "integer", "null": True},
            {"name": "jitter_percentile_60", "type": "integer", "null": True},
            {"name": "jitter_percentile_70", "type": "integer", "null": True},
            {"name": "jitter_percentile_80", "type": "integer", "null": True},
            {"name": "jitter_percentile_90", "type": "integer", "null": True},
            {"name": "jitter_percentile_100", "type": "integer", "null": True},
            {"name": "packets_sent", "type": "integer", "null": False},
            {"name": "packets_recvd", "type": "integer", "null": False},
            #{"name": "loss_periods", "type": "varchar", "null": True},
            {"name": "itu_mos", "type": "float", "null": True},

        ]

        # Not strictly correct to take the mean of the means, but hard to do
        # much else without some custom functions
        aggs = [("mean_rtt", "mean", "mean_rtt"),
                ("mean_jitter", "mean", "mean_jitter"),
                ("jitter_percentile_10", "mean", "jitter_percentile_10"),
                ("jitter_percentile_20", "mean", "jitter_percentile_20"),
                ("jitter_percentile_30", "mean", "jitter_percentile_30"),
                ("jitter_percentile_40", "mean", "jitter_percentile_40"),
                ("jitter_percentile_50", "mean", "jitter_percentile_50"),
                ("jitter_percentile_60", "mean", "jitter_percentile_60"),
                ("jitter_percentile_70", "mean", "jitter_percentile_70"),
                ("jitter_percentile_80", "mean", "jitter_percentile_80"),
                ("jitter_percentile_90", "mean", "jitter_percentile_90"),
                ("jitter_percentile_100", "mean", "jitter_percentile_100"),
                ("packets_sent", "sum", "packets_sent"),
                ("packets_recvd", "sum", "packets_recvd"),
                ("itu_mos", "mean", "itu_mos")
               ]

        self.cqs = [
            (['5m', '10m', '20m', '40m', '80m', '4h'],
            aggs)
        ]

    def create_existing_stream(self, stream_data):
        key = self._construct_key(stream_data)
        streamid = stream_data["stream_id"]
        self.streams[key] = streamid

    def _construct_key(self, streamdata):
        src = str(streamdata["source"])
        dest = str(streamdata["destination"])
        address = streamdata["address"]
        direction  = str(streamdata["direction"])
        packet_size = str(streamdata["packet_size"])
        packet_spacing = str(streamdata["packet_spacing"])
        packet_count = str(streamdata["packet_count"])
        dscp = str(streamdata["dscp"])

        return (src, dest, address, direction, packet_size, packet_spacing, \
                packet_count, dscp)


    def _process_single_result(self, timestamp, resdict):
        key = self._construct_key(resdict)

        if key in self.streams:
            stream_id = self.streams[key]
        else:
            stream_id = self.create_new_stream(resdict, timestamp)

            if stream_id < 0:
                logger.log("AMPModule: Cannot create new UDPstream stream")
                logger.log("AMPModule: %s:%s:%s" % (\
                        resdict['source'], resdict['destination'],
                        resdict['direction']))
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
            resdict['direction'] = result['direction']
            resdict['packet_size'] = data['packet_size']
            resdict['packet_spacing'] = data['packet_spacing']
            resdict['packet_count'] = data['packet_count']
            resdict['dscp'] = data['dscp']

            resdict['mean_rtt'] = result['rtt']['mean']
            resdict['mean_jitter'] = result['jitter']['mean']
            resdict['packets_recvd'] = result['packets_received']
            resdict['packets_sent'] = data['packet_count']
            resdict['itu_mos' ] = result['voip']['itu_mos']

            resdict['jitter_percentile_10'] = result['percentiles'][0]
            resdict['jitter_percentile_20'] = result['percentiles'][1]
            resdict['jitter_percentile_30'] = result['percentiles'][2]
            resdict['jitter_percentile_40'] = result['percentiles'][3]
            resdict['jitter_percentile_50'] = result['percentiles'][4]
            resdict['jitter_percentile_60'] = result['percentiles'][5]
            resdict['jitter_percentile_70'] = result['percentiles'][6]
            resdict['jitter_percentile_80'] = result['percentiles'][7]
            resdict['jitter_percentile_90'] = result['percentiles'][8]
            resdict['jitter_percentile_100'] = result['percentiles'][9]

            streamid = self._process_single_result(timestamp, resdict)
            if streamid < 0:
                return
            done[streamid] = 0

        self.db.update_timestamp(self.datatable, done.keys(), timestamp)

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
