# This file is part of NNTSC
#
# Copyright (C) 2013-2014 The University of Waikato, Hamilton, New Zealand
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

from libnntsc.parsers.amp_icmp import AmpIcmpParser
from libnntsc.dberrorcodes import *
import libnntscclient.logger as logger


class AmpTcppingParser(AmpIcmpParser):
    def __init__(self, db):
        super(AmpTcppingParser, self).__init__(db)

        self.streamtable = "streams_amp_tcpping"
        self.datatable = "data_amp_tcpping"
        self.colname = "amp_tcpping"
        self.source = "amp"
        self.module = "tcpping"

        self.streamcolumns = [
            {"name":"source", "type":"varchar", "null":False},
            {"name":"destination", "type":"varchar", "null":False},
            {"name":"port", "type":"integer", "null":False},
            {"name":"family", "type":"varchar", "null":False},
            {"name":"packet_size", "type":"varchar", "null":False},
        ]
        
        self.uniquecolumns = ['source', 'destination', 'port', 'family',
                'packet_size']
        self.streamindexes = [
            {"name": "", "columns": ['source']},
            {"name": "", "columns": ['destination']},
            {"name": "", "columns": ['port']}
        ]

        self.datacolumns = [
            {"name":"median", "type":"integer", "null":True},
            {"name":"packet_size", "type":"smallint", "null":False},
            {"name":"loss", "type":"smallint", "null":False},
            {"name":"icmperrors", "type":"smallint", "null":False},
            {"name":"rtts", "type":"integer[]", "null":True},
            #{"name":"replyflags", "type":"smallint", "null":True},
            #{"name":"icmptype", "type":"smallint", "null":True},
            #{"name":"icmpcode", "type":"smallint", "null":True},
        ]

        #self.dataindexes = []

    def create_existing_stream(self, stream_data):
        src = str(stream_data['source'])
        dest = str(stream_data['destination'])
        family = str(stream_data['family'])
        port = str(stream_data['port'])
        size = str(stream_data['packet_size'])

        key = (src, dest, port, family, size)
        self.streams[key] = stream_data['stream_id']

    def _stream_properties(self, source, result):
        props = {}

        if 'target' not in result:
            logger.log("Error: no target specified in %s result" % \
                    (self.colname))
            return None, None

        if 'port' not in result:
            logger.log("Error: no port specified in %s result" % \
                    (self.colname))
            return None, None

        if 'address' not in result:
            logger.log("Error: no address specified in %s result" % \
                    (self.colname))
            return None, None
        
        if '.' in result['address']:
            family = "ipv4"
        else:
            family = "ipv6"

        if result['random']:
            sizestr = "random"
        else:
            if 'packet_size' not in result:
                logger.log("Error: no packet size specified in %s result" % \
                        (self.colname))
                return None, None
            sizestr = str(result['packet_size'])

        props['source'] = source
        props['destination'] = result['target']
        props['port'] = str(result['port'])
        props['family'] = family
        props['packet_size'] = sizestr

        key = (props['source'], props['destination'], props['port'], \
                props['family'], props['packet_size'])
        return props, key


    def _update_stream(self, observed, streamid, datapoint):
        if streamid not in observed:
            observed[streamid] = { "loss":0, "rtts":[], "icmperrors":0,
                    "median":None, "packet_size":datapoint["packet_size"] }

        if 'reply' in datapoint and datapoint['reply'] == 2:
            observed[streamid]["icmperrors"] += 1

        if 'loss' in datapoint:
            observed[streamid]["loss"] += datapoint['loss']

        if 'rtt' in datapoint and datapoint['rtt'] is not None:
            observed[streamid]["rtts"].append(datapoint['rtt'])

    def _aggregate_streamdata(self, streamdata):
        streamdata["rtts"].sort()
        streamdata["median"] = self._find_median(streamdata["rtts"])

        # Add None entries to our array for lost measurements -- we
        # have to wait until now to add them otherwise they'll mess
        # with our median calculation
        nulls = [None] * (streamdata['loss'] + streamdata['icmperrors'])
        streamdata["rtts"] += nulls
 
        

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
