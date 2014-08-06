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

        self.collections = {}

    def __del__(self):
        self.mcpool.relinquish()

    def update_timestamps(self, collection, streamid, last, first=None):
        if first == None and last == None:
            return
       
        if last is not None:
            self._update_last_timestamp(collection, streamid, last)

        # Don't bother trying to update 'first' -- if anyone wants it
        # and it is uncached, it's probably less effort to do the query
        # than update the cache every time we create a new stream
        #if first is not None:
        #    self._update_first_timestamp(collection, streamid, first)
        
    def _update_first_timestamp(self, collection, streamid, first):
        # Always fetch first timestamps, because another process might
        # set the first timestamp instead
        coldict = self._fetch_dict(collection, "first")
        coldict[streamid] = first
        self.set_first_timestamps(collection, coldict)

    def _update_last_timestamp(self, collection, streamid, last):    
        if collection not in self.collections:
            coldict = self._fetch_dict(collection, "last")
            self.collections[collection] = {"streams":coldict}
        else:
            coldict = self.collections[collection]['streams']

        if streamid not in coldict or last > coldict[streamid]:
            coldict[streamid] = last

        if 'laststore' not in self.collections[collection]:
            self.collections[collection]['laststore'] = last

        # Write timestamps back to the cache every 5 mins rather than 
        # every time we update a stream, otherwise this gets very slow
        if last - self.collections[collection]['laststore'] >= 300:
            self.set_last_timestamps(collection, coldict)
            self.collections[collection]['laststore'] = last

    def fetch_all_last_timestamps(self, collection):
        fetched = self._fetch_dict(collection, "last")
        return fetched

    def fetch_all_first_timestamps(self, collection):
        fetched = self._fetch_dict(collection, "first")
        return fetched

    def _fetch_dict(self, collection, style):

        key = self._dict_cache_key(collection, style)
       
        #print "Fetching using key", key 
        coldict = {}
        with self.mcpool.reserve() as mc:
            try:
                if key in mc:
                    coldict = mc.get(key)
            except pylibmc.SomeErrors as e:
                log("Warning: pylibmc error while fetching collection timestamps")
                log(e)

        return coldict

    def set_first_timestamps(self, collection, coldict):
        self._set_timestamps(collection, coldict, "first")
    
    def set_last_timestamps(self, collection, coldict):
        self._set_timestamps(collection, coldict, "last")

    def _set_timestamps(self, collection, coldict, style):
        key = self._dict_cache_key(collection, style)
        
        #print "Storing using key", key 
        with self.mcpool.reserve() as mc:
            try:
                mc.set(key, coldict, self.cachetime)
            except pylibmc.SomeErrors as e:
                log("Warning: pylibmc error while storing collection timestamps")
                log(e)

    
    def _dict_cache_key(self, collection, style):
        return "nntsc_%s_%s_%s" % (self.nntscid, str(collection), style)

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
