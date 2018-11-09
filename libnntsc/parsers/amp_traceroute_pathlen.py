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


from libnntsc.dberrorcodes import DB_DATA_ERROR
import libnntscclient.logger as logger
from libnntsc.parsers.amp_icmp import AmpIcmpParser

class AmpTraceroutePathlenParser(AmpIcmpParser):
    def __init__(self, db, influxdb=None):
        super(AmpTraceroutePathlenParser, self).__init__(db, influxdb)

        self.streamtable = "streams_amp_traceroute"
        self.datatable = "data_amp_traceroute_pathlen"
        self.colname = "amp_traceroute_pathlen"
        self.source = "amp"
        self.module = "traceroute_pathlen"

        self.datacolumns = [
            {"name":"path_length", "type":"float", "null":False},
        ]

        self.matrix_cq = [
            ("path_length", "mode", "path_length"),
        ]


    def process_data(self, timestamp, data, source):
        """ Process a AMP traceroute message, which can contain 1 or more
            sets of results
        """
        lengthseen = {}

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

            self._extract_paths(d)

            # IP flag tells us if this is intended as an IP traceroute.
            # If the flag isn't present, we're running an old ampsave
            # that pre-dates AS path support so assume an IP traceroute in
            # that case
            if 'ip' not in d or d['ip'] != 0:

                if d['path'] is not None and d['path'][-1] is None:
                    d['length'] += 0.5

                    while len(d['path']) > 0 and d['path'][-1] is None:
                        d['path'] = d['path'][:-1]
                        d['length'] -= 1

                elif d['path'] is not None:
                    d['length'] += 0.0
                else:
                    d['length'] = 0.0


                if streamid not in lengthseen:
                    lengthseen[streamid] = {d['length']: 1}
                elif d['length'] not in lengthseen[streamid]:
                    lengthseen[streamid][d['length']] = 1
                else:
                    lengthseen[streamid][d['length']] += 1

            elif 'as' in d and d['as'] != 0:

                if d['aspath'] is not None and "-" in d['aspath'][-1]:
                    d['responses'] += 0.5
                elif d['responses'] is not None:
                    d['responses'] += 0.0
                else:
                    d['responses'] = 0.0

                if streamid not in lengthseen:
                    lengthseen[streamid] = {d['responses']: 1}
                elif d['responses'] not in lengthseen[streamid]:
                    lengthseen[streamid][d['responses']] = 1
                else:
                    lengthseen[streamid][d['responses']] += 1

        for sid, lengths in lengthseen.iteritems():
            modelen = None
            modelencount = 0.0

            for l, c in lengths.iteritems():
                if c > modelencount:
                    modelencount = c
                    if l is not None:
                        modelen = float(l)

            toinsert = {'path_length': modelen}

            #print toinsert, sid, lengths
            self.insert_data(sid, timestamp, toinsert)

        # update the last timestamp for all streams we just got data for
        self.db.update_timestamp(self.datatable, lengthseen.keys(),
                timestamp, False)

    def _extract_paths(self, result):
        aspath = []
        ippath = []
        rtts = []
        currentas = None
        responses = 0
        count = 0
        aspathlen = 0

        seenas = []

        for x in result['hops']:
            if 'address' in x:
                ippath.append(x['address'])
            else:
                ippath.append(None)

            if 'rtt' in x:
                rtts.append(x['rtt'])
            else:
                rtts.append(None)

            if 'as' not in x:
                continue

            if currentas != x['as']:
                if currentas != None:
                    assert(count != 0)
                    aspath.append("%d.%d" % (count, currentas))
                currentas = x['as']
                count = 1
            else:
                count += 1

            # Keep track of unique AS numbers in the path, not counting
            # null hops, RFC 1918 addresses or failed lookups
            if x['as'] not in seenas and x['as'] >= 0:
                seenas.append(x['as'])

            aspathlen += 1
            responses += 1

        if currentas != None:
            assert(count != 0)
            assert(responses >= count)

            aspath.append("%d.%d" % (count, currentas))

            # Remove tailing "null hops" from our responses count
            if currentas == -1:
                responses -= count

        if len(rtts) == 0:
            result["hop_rtt"] = None
        else:
            result['hop_rtt'] = rtts

        if len(aspath) == 0:
            result["aspath"] = None
            result["aspathlen"] = None
            result["uniqueas"] = None
            result["responses"] = None
        else:
            result["aspath"] = aspath
            result["aspathlen"] = aspathlen
            result["uniqueas"] = len(seenas)
            result["responses"] = responses

        if len(ippath) == 0:
            result["path"] = None
        else:
            result['path'] = ippath



# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
