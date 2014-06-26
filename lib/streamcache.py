import time
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

    def __del__(self):
        self.mcpool.relinquish()

    def store_firstts(self, streamid, timestamp):
        self._store(streamid, timestamp, "firstts")

    def store_stream(self, streamid, timestamp):
        self._store(streamid, timestamp, "lastts")

    def fetch_firstts(self, streamid):
        return self._fetch(streamid, "firstts")

    def fetch_stream(self, streamid):
        return self._fetch(streamid, "lastts")

    def _store(self, streamid, timestamp, tstype):
        key = self._cache_key(streamid, tstype)
        #print "store", key, self.cachetime, timestamp

        with self.mcpool.reserve() as mc:
            try:
                mc.set(key, timestamp, self.cachetime)
            except pylibmc.SomeErrors as e:
                log("Warning: pylibmc error while storing stream")
                log(e)


    def _fetch(self, streamid, tstype):
        key = self._cache_key(streamid, tstype)
        #print "fetch", key

        with self.mcpool.reserve() as mc:
            try:
                if key in mc:
                    return mc.get(key)
            except pylibmc.SomeErrors as e:
                log("Warning: pylibmc error while fetching stream")
                log(e)

        return -1


    def _cache_key(self, streamid, tstype):
        return "nntsc_%s_%s_%s" % (self.nntscid, tstype, str(streamid))

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
