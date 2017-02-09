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
            {"name":"lossrate", "type":"float", "null":False},
            {"name":"rtts", "type":"integer[]", "null":True},
        ]

        self.dataindexes = [
        ]

        self.matrix_cq = [
            ("median", "mean", "median_avg"),
            ("median", "stddev", "median_stddev"),
            ("median", "count", "median_count"),
            ("loss", "sum", "loss_sum"),
            ("results", "sum", "results_sum"),
            ("lossrate", "stddev", "lossrate_stddev"),
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
                    "results":0}

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

        if streamdata["results"] > 0:
            streamdata["lossrate"] = streamdata["loss"] / float(streamdata["results"])
        else:
            streamdata["lossrate"] = 0.0

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
        self.db.update_timestamp(self.datatable, observed.keys(), timestamp,
                self.have_influx)

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
