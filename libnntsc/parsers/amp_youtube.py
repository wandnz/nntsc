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

import json
from libnntsc.parsers.common import NNTSCParser
import libnntscclient.logger as logger

class AmpYoutubeParser(NNTSCParser):
    def __init__(self, db, influxdb=None):
        super(AmpYoutubeParser, self).__init__(db, influxdb)

        self.streamtable = "streams_amp_youtube"
        self.datatable = "data_amp_youtube"
        self.colname = "amp_youtube"
        self.source = "amp"
        self.module = "youtube"

        self.streamcolumns = [
            {"name":"source", "type":"varchar", "null":False},
            {"name":"destination", "type":"varchar", "null":False},
            {"name":"quality", "type":"integer", "null":False},
        ]

        self.uniquecolumns = ['source', 'destination', 'quality']
        self.streamindexes = [
            {"name": "", "columns": ['source']},
            {"name": "", "columns": ['destination']}
        ]

        self.datacolumns = [
            {"name":"total_time", "type":"integer", "null":True},
            {"name":"pre_time", "type":"integer", "null":True},
            {"name":"initial_buffering", "type":"integer", "null":True},
            {"name":"playing_time", "type":"integer", "null":True},
            {"name":"stall_time", "type":"integer", "null":True},
            {"name":"stall_count", "type":"integer", "null":True},
            #{"name":"timeline", "type":"varchar", "null":True},
        ]

        self.dataindexes = [
        ]

        self.matrix_cq = [
            ('"total_time"', 'mean', '"total_time_avg"'),
            ('"total_time"', 'stddev', '"total_time_stddev"'),
            ('"pre_time"', 'mean', '"pre_time_avg"'),
            ('"pre_time"', 'stddev', '"pre_time_stddev"'),
            ('"initial_buffering"', 'mean', '"initial_buffering_avg"'),
            ('"initial_buffering"', 'stddev', '"initial_buffering_stddev"'),
            ('"stall_time"', 'mean', '"stall_time_avg"'),
            ('"stall_time"', 'stddev', '"stall_time_stddev"'),
            ('"stall_count"', 'mean', '"stall_count_avg"'),
            ('"stall_count"', 'stddev', '"stall_count_stddev"'),
        ]


    def _stream_key(self, stream_data):
        src = str(stream_data["source"])

        if 'video' in stream_data:
            dest = str(stream_data["video"])
        else:
            dest = str(stream_data['destination'])

        # XXX which quality? requested vs actual
        if 'requested_quality' in stream_data:
            quality = str(stream_data['requested_quality'])
        else:
            quality = str(stream_data['quality'])

        key = (src, dest, quality)

        return key

    def create_existing_stream(self, stream_data):
        """Extract the stream key from the stream data provided by NNTSC
    when the AMP module is first instantiated"""

        key = self._stream_key(stream_data)
        self.streams[key] = stream_data['stream_id']

    def _mangle_result(self, data):
        # XXX is this mangling really needed? most other tests don't have
        # a special function, we just have this here cause of the test
        # I copied to make this one go

        # Our columns are slightly different to the names that AMPsave uses,
        # so we'll have to mangle them to match what we're expecting
        key = self._stream_key(data)

        mangled = {}
        mangled['source'] = key[0]
        mangled['destination'] = key[1]
        mangled['quality'] = key[2]

        mangled['total_time'] = int(data['total_time'])
        mangled['pre_time'] = int(data['pre_time'])
        mangled['initial_buffering'] = int(data['initial_buffering'])
        mangled['playing_time'] = int(data['playing_time'])
        mangled['stall_time'] = int(data['stall_time'])
        mangled['stall_count'] = int(data['stall_count'])
        #mangled['timeline'] = json.dumps(data['timeline'])

        return mangled, key

    def process_data(self, timestamp, data, source):
        data['source'] = source

        mangled, key = self._mangle_result(data)

        if key in self.streams:
            stream_id = self.streams[key]
        else:
            stream_id = self.create_new_stream(mangled, timestamp,
                    not self.have_influx)
            if stream_id < 0:
                logger.log("AMPModule: Cannot create stream for: ")
                logger.log("AMPModule: youtube %s %s\n", source, \
                        mangled['destination'])
            self.streams[key] = stream_id

        self.insert_data(stream_id, timestamp, mangled)
        self.db.update_timestamp(self.datatable, [stream_id], timestamp,
                self.have_influx)

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
