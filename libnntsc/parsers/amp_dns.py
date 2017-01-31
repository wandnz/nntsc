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
import libnntscclient.logger as logger
from libnntsc.parsers.common import NNTSCParser

class AmpDnsParser(NNTSCParser):
    def __init__(self, db, influxdb=None):
        super(AmpDnsParser, self).__init__(db, influxdb)

        self.streamtable = "streams_amp_dns"
        self.datatable = "data_amp_dns"
        self.colname = "amp_dns"
        self.source = "amp"
        self.module = "dns"

        self.streamcolumns = [
            {"name":"source", "type":"varchar", "null":False},
            {"name":"destination", "type":"varchar", "null":False},
            {"name":"instance", "type":"varchar", "null":False},
            {"name":"address", "type":"inet", "null":False},
            {"name":"query", "type":"varchar", "null":False},
            {"name":"query_type", "type":"varchar", "null":False},
            {"name":"query_class", "type":"varchar", "null":False},
            {"name":"udp_payload_size", "type":"integer", "null":False},
            {"name":"recurse", "type":"boolean", "null":False},
            {"name":"dnssec", "type":"boolean", "null":False},
            {"name":"nsid", "type":"boolean", "null":False}
        ]

        self.uniquecolumns = [
            'source', 'destination', 'query', 'address', 'query_type',
            'query_class', 'udp_payload_size', 'recurse', 'dnssec', 'nsid',
            'instance']

        self.streamindexes = [
            {"name":"", "columns":['source']},
            {"name":"", "columns":['destination']},
            {"name":"", "columns":['query']},
        ]

        self.datacolumns = [
            {"name":"response_size", "type":"integer", "null":True},
            {"name":"rtt", "type":"integer", "null":True},
            {"name":"ttl", "type":"smallint", "null":True},
            {"name":"query_len", "type":"smallint", "null":True},
            {"name":"total_answer", "type":"smallint", "null":True},
            {"name":"total_authority", "type":"smallint", "null":True},
            {"name":"total_additional", "type":"smallint", "null":True},
            {"name":"opcode", "type":"smallint", "null":True},
            {"name":"rcode", "type":"smallint", "null":True},
            {"name":"flag_rd", "type":"boolean", "null":True},
            {"name":"flag_tc", "type":"boolean", "null":True},
            {"name":"flag_aa", "type":"boolean", "null":True},
            {"name":"flag_qr", "type":"boolean", "null":True},
            {"name":"flag_cd", "type":"boolean", "null":True},
            {"name":"flag_ad", "type":"boolean", "null":True},
            {"name":"flag_ra", "type":"boolean", "null":True},
            {"name":"requests", "type":"smallint", "null":False},
            {"name":"lossrate", "type":"float", "null": False},
        ]

        self.dataindexes = [
            {"name": "", "columns":['rtt']}
        ]

        self.matrix_cq = [
            ('rtt', 'mean', 'rtt_avg'),
            ('rtt', 'stddev', 'rtt_stddev'),
            ('rtt', 'count', 'rtt_count'),
            ('requests', 'sum', 'requests_count'),
            ('lossrate', 'stddev', 'lossrate_stddev')
        ]


    def _result_to_key(self, res):
        key = (str(res['source']), str(res['destination']), 
                str(res['instance']),
                res['address'], str(res['query']), str(res['query_type']),
                str(res['query_class']), str(res['udp_payload_size']),
                res['recurse'], res['dnssec'], res['nsid'])

        return key

    def create_existing_stream(self, stream_data):
        key = self._result_to_key(stream_data)
        self.streams[key] = stream_data['stream_id']


    def process_data(self, timestamp, data, source):
        done = {}

        for r in data['results']:
            streamresult, dataresult = self._split_result(data, r)

            # Source is not part of the AMP result itself
            streamresult['source'] = source
            key = self._result_to_key(streamresult)

            if key in self.streams:
                stream_id = self.streams[key]

                if stream_id in done:
                    continue
            else:
                stream_id = self.create_new_stream(streamresult, timestamp,
                        not self.have_influx)
                if stream_id < 0:
                    logger.log("AMPModule: Cannot create stream for:")
                    logger.log("AMPModule: %s %s %s %s\n" % ("dns", source,
                            streamresult['destination'], streamresult['query']))
                    return stream_id
                self.streams[key] = stream_id

            dataresult['requests'] = 1
            if 'response_size' in dataresult and dataresult['response_size']:
                dataresult['lossrate'] = 0.0
            else:
                dataresult['lossrate'] = 1.0

            self.insert_data(stream_id, timestamp, dataresult)
            done[stream_id] = 0

        self.db.update_timestamp(self.datatable, done.keys(), timestamp,
                self.have_influx)


    def _split_result(self, alldata, result):

        streamkeys = [x['name'] for x in self.streamcolumns]
        flagnames = ['rd', 'tc', 'aa', 'qr', 'cd', 'ad', 'ra']

        stream = {}
        data = {}

        for k, v in alldata.items():
            if k == "results":
                continue
            stream[k] = v

        for k, v in result.items():
            if k in streamkeys:
                stream[k] = v

            elif k == "flags":
                for f, fval in v.items():
                    if f in flagnames:
                        data["flag_" + f] = fval
                    else:
                        data[f] = fval
            else:
                data[k] = v

        return stream, data


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
