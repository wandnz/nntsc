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

import time
import pickle
import zlib
import pylibmc

from libnntscclient.logger import *

class StreamCache(object):
    def __init__(self, nntscid, cachetime):
        self.memcache = pylibmc.Client(
            ["127.0.0.1"],
            behaviors={
                "tcp_nodelay": True,
                "no_block": True,
            })
        self.mcpool = pylibmc.ThreadMappedPool(self.memcache)
        self.nntscid = nntscid
        self.cachetime = int(cachetime)

        self.collections = {}

    def __del__(self):
        self.mcpool.relinquish()

    def update_timestamps(self, db, collection, streamids, last, first=None):
        if first is None and last is None:
            return

        if last is not None:
            self._update_last_timestamp(db, collection, streamids, last)

        # Don't bother trying to update 'first' -- if anyone wants it
        # and it is uncached, it's probably less effort to do the query
        # than update the cache every time we create a new stream
        #if first is not None:
        #    self._update_first_timestamp(collection, streamid, first)

    def _update_first_timestamp(self, db, collection, streamid, first):
        # Always fetch first timestamps, because another process might
        # set the first timestamp instead
        coldict = self._fetch_dict(db, collection, "first")
        coldict[streamid] = first
        self.set_first_timestamps(db, collection, coldict)

    def _update_last_timestamp(self, db, collection, streamids, last):
        if collection not in self.collections:
            coldict = self._fetch_dict(db, collection, "last")
            self.collections[collection] = {"streams":coldict}
        else:
            coldict = self.collections[collection]['streams']

        for s in streamids:
            if s not in coldict or last > coldict[s]:
                coldict[s] = last

        now = time.time()
        if 'laststore' not in self.collections[collection]:
            self.set_last_timestamps(db, collection, coldict)
            self.collections[collection]['laststore'] = time.time()

        # Write timestamps back to the cache every 5 mins rather than
        # every time we update a stream, otherwise this gets very slow
        if now - self.collections[collection]['laststore'] >= 60:
            self.set_last_timestamps(db, collection, coldict)
            self.collections[collection]['laststore'] = now

    def fetch_all_last_timestamps(self, db, collection):
        fetched = self._fetch_dict(db, collection, "last")
        return fetched

    def fetch_all_first_timestamps(self, db, collection):
        fetched = self._fetch_dict(db, collection, "first")
        return fetched

    def _fetch_dict(self, db, collection, style):

        key = self._dict_cache_key(db, collection, style)

        #print "Fetching using key", key, time.time()
        coldict = {}
        with self.mcpool.reserve() as mc:
            try:
                if key in mc:
                    fetched = mc.get(key)
                    coldict = pickle.loads(zlib.decompress(fetched))
            except pylibmc.SomeErrors as e:
                log("Warning: pylibmc error while fetching collection timestamps")
                log(e)

        return coldict

    def set_first_timestamps(self, db, collection, coldict):
        self._set_timestamps(db, collection, coldict, "first")

    def set_last_timestamps(self, db, collection, coldict):
        self._set_timestamps(db, collection, coldict, "last")

    def _set_timestamps(self, db, collection, coldict, style):
        key = self._dict_cache_key(db, collection, style)

        tostore = zlib.compress(pickle.dumps(coldict), 1)
        #print "Storing using key", key, time.time()
        with self.mcpool.reserve() as mc:
            try:
                mc.set(key, tostore, self.cachetime)
            except pylibmc.SomeErrors as e:
                log("Warning: pylibmc error while storing collection timestamps")
                log(e)


    def _dict_cache_key(self, db, collection, style):
        return "nntsc_%s_%s_%s_%s" % (self.nntscid, db, str(collection), style)

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
