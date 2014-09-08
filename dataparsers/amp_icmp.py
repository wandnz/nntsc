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

from libnntsc.parsers.common import NNTSCParser
from libnntsc.dberrorcodes import *
import libnntscclient.logger as logger

class AmpIcmpParser(NNTSCParser):
    def __init__(self, db):
        super(AmpIcmpParser, self).__init__(db)

        self.streamtable = "streams_amp_icmp"
        self.datatable = "data_amp_icmp"
        self.colname = "amp_icmp"
        self.source = "amp"
        self.module = "icmp"

        self.streamcolumns = [
            {"name":"source", "type":"varchar", "null":False},
            {"name":"destination", "type":"varchar", "null":False},
            {"name":"address", "type":"inet", "null":False},
            {"name":"packet_size", "type":"varchar", "null":False},
            {"name":"datastyle", "type":"varchar", "null":False}
        ]

        self.uniquecolumns = ['source', 'destination', 'packet_size', 
                'address']
        self.streamindexes = [
            {"name": "", "columns": ['source']},
            {"name": "", "columns": ['destination']}
        ]

        self.datacolumns = [
            {"name":"rtt", "type":"integer", "null":True},
            {"name":"packet_size", "type":"smallint", "null":False},
            {"name":"ttl", "type":"smallint", "null":True},
            {"name":"loss", "type":"smallint", "null":False},
            {"name":"error_type", "type":"smallint", "null":True},
            {"name":"error_code", "type":"smallint", "null":True},
        ]

        self.dataindexes = [
            {"name": "", "columns":['rtt']}
        ] 


    def create_existing_stream(self, stream_data):
        """Extract the stream key from the stream data provided by NNTSC
    when the AMP module is first instantiated"""

        src = str(stream_data["source"])
        dest = str(stream_data["destination"])
        addr = str(stream_data["address"])
        size = str(stream_data["packet_size"])
        streamid = stream_data["stream_id"]

        key = (src, dest, addr, size)

        self.streams[key] = streamid

    def _stream_properties(self, source, result):

        props = {}
        if 'target' not in result:
            logger.log("Error: no target specified in %s result" % \
                    (self.colname))
            return None, None

        if 'address' not in result:
            logger.log("Error: no address specified in %s result" % \
                    (self.colname))
            return None, None

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
        props['address']  = result['address']
        props['packet_size'] = sizestr
        props['datastyle'] = "rtt_ms"

        key = (props['source'], props['destination'], props['address'], \
                props['packet_size'])
        return props, key

    def _mangle_result(self, result):
        # Perform any modifications to the test result structure to ensure
        # that it matches the format expected by our database
        #
        # No mangling is necessary for AMP-ICMP, but it is required by
        # amp-traceroute which will inherit from us so we need to define
        # this function here
        return 1

    def process_data(self, timestamp, data, source):
        """ Process a AMP ICMP message, which can contain 1 or more sets of 
            results 
        """
        done = {}

        for d in data:
            streamparams, key = self._stream_properties(source, d)

            if key is None:
                logger.log("Failed to determine stream for %s result" % \
                        (self.colname))
                return DB_DATA_ERROR

            if key not in self.streams:
                streamid = self.create_new_stream(streamparams, timestamp)
                if streamid < 0:
                    logger.log("Failed to create new %s stream" % \
                            (self.colname))
                    logger.log("%s" % (str(streamparams)))
                    return
                self.streams[key] = streamid
            else:
                streamid = self.streams[key]

                if streamid in done:
                    continue

            if self._mangle_result(d):
                self.insert_data(streamid, timestamp, d)
                done[streamid] = 0

        # update the last timestamp for all streams we just got data for
        self.db.update_timestamp(self.datatable, done.keys(), timestamp)

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :

