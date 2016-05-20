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
from copy import deepcopy
import libnntscclient.logger as logger

class AmpIcmpParser(NNTSCParser):
    def __init__(self, db, influxdb=None):
        super(AmpIcmpParser, self).__init__(db, influxdb)

        self.influxdb = influxdb
        
        self.streamtable = "streams_amp_icmp"
        self.datatable = "data_amp_icmp"
        self.colname = "amp_icmp"
        self.source = "amp"
        self.module = "icmp"

        self.streamcolumns = [
            {"name":"source", "type":"varchar", "null":False},
            {"name":"destination", "type":"varchar", "null":False},
            {"name":"family", "type":"varchar", "null":False},
            {"name":"packet_size", "type":"varchar", "null":False},
        ]

        self.uniquecolumns = ['source', 'destination', 'packet_size', 
                'family']
        self.streamindexes = [
            {"name": "", "columns": ['source']},
            {"name": "", "columns": ['destination']}
        ]

        self.datacolumns = [
            {"name":"median", "type":"integer", "null":True},
            {"name":"packet_size", "type":"smallint", "null":False},
            {"name":"loss", "type":"smallint", "null":False},
            {"name":"results", "type":"smallint", "null":False},
            {"name":"rtts", "type":"integer[]", "null":True},
        ]

        self.dataindexes = [
        ]

        aggs  =  [
            ("loss","sum","loss"),
            ("num_results","sum","results"),
            ("mean_rtt", "mean", "median"),
            ("stddev_rtt", "stddev", "median"),
            ("max_rtt", "max", "median"),
            ("min_rtt","min","median")
          ]

        aggs_w_ntile = deepcopy(aggs)
        aggs_w_ntile += (
              [("\"{}_percentile_rtt\"".format(
                      i), "percentile", "median, {}".format(i)) for i in range(5,100,5)]
        )
        
        self.cqs = [
#            (['1h','1d'],
#            aggs),
            (['5m','10m','20m','40m','80m','4h'],
             aggs_w_ntile)
            ]

    def create_existing_stream(self, stream_data):
        """Extract the stream key from the stream data provided by NNTSC
    when the AMP module is first instantiated"""

        src = str(stream_data["source"])
        dest = str(stream_data["destination"])
        family = str(stream_data["family"])
        size = str(stream_data["packet_size"])
        streamid = stream_data["stream_id"]

        key = (src, dest, family, size)

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
        props['family']  = family
        props['packet_size'] = sizestr

        key = (props['source'], props['destination'], props['family'], \
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

    def _update_stream(self, observed, streamid, datapoint):
        if streamid not in observed:
            observed[streamid] = { "loss":0, "rtts":[], 
                    "median":None, "packet_size":datapoint["packet_size"],
                    "results":0 }

        observed[streamid]["results"] += 1

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
        streamdata["rtts"] += [None] * streamdata['loss']

    def process_data(self, timestamp, data, source):
        """ Process a AMP ICMP message, which can contain 1 or more sets of 
            results 
        """
        observed = {}

        for d in data:
            streamparams, key = self._stream_properties(source, d)

            if key is None:
                logger.log("Failed to determine stream for %s result" % \
                        (self.colname))
                return DB_DATA_ERROR

            if key not in self.streams:
                streamid = self.create_new_stream(streamparams, timestamp,
                        not self.have_influx)
                if streamid < 0:
                    logger.log("Failed to create new %s stream" % \
                            (self.colname))
                    logger.log("%s" % (str(streamparams)))
                    return
                self.streams[key] = streamid
            else:
                streamid = self.streams[key]

            self._update_stream(observed, streamid, d)

        for sid, streamdata in observed.iteritems():
            self._aggregate_streamdata(streamdata)

            if self.influxdb:
                casts = {"rtts":str}
            else:
                casts = {"rtts":"integer[]"}
            self.insert_data(sid, timestamp, streamdata, casts)

        # update the last timestamp for all streams we just got data for
        self.db.update_timestamp(self.datatable, observed.keys(), timestamp)

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :

