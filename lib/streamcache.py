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
        
        if collection not in self.collections:
            coldict = self._fetch_dict(collection)
            self.collections[collection] = {"streams":coldict}
        else:
            coldict = self.collections[collection]['streams']

        self._store_timestamps(coldict, streamid, last, first)

        if last is None:
            return

        if 'laststore' not in self.collections[collection]:
            self.collections[collection]['laststore'] = last

        # Write timestamps back to the cache every 5 mins rather than 
        # every time we update a stream, otherwise this gets very slow
        if last - self.collections[collection]['laststore'] >= 300:
            self.set_timestamps(collection, coldict)
            self.collections[collection]['laststore'] = last

    def _store_timestamps(self, coldict, streamid, last, first=None):
        if streamid not in coldict:
            coldict[streamid] = (first, last)
        else:
            stamps = coldict[streamid]
            # Don't overwrite the lasttimestamp if a more recent value
            # is in the cache
            if last is not None and last < stamps[1]:
                last = stamps[1]
            
            if first == None:
                stamps = (stamps[0], last)
            elif last == None:
                stamps = (first, stamps[1])
            else:
                stamps = (first, last)
            coldict[streamid] = stamps

    def fetch_timestamp_dict(self, collection):
        fetched = self._fetch_dict(collection)
        return fetched

    def fetch_timestamps(self, collection, streamid):
        if collection not in self.collections:
            coldict = self._fetch_dict(collection)
            self.collections[collection] = {"streams":coldict}
        else:
            coldict = self.collections[collection]['streams']

        #coldict = self._fetch_dict(collection)
        if streamid not in coldict:
            return (None, None)
        
        return coldict[streamid]
        
    def _fetch_dict(self, collection):

        key = self._dict_cache_key(collection)
       
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

    def set_timestamps(self, collection, coldict):
        key = self._dict_cache_key(collection)
        
        #print "Storing using key", key 
        with self.mcpool.reserve() as mc:
            try:
                mc.set(key, coldict, self.cachetime)
            except pylibmc.SomeErrors as e:
                log("Warning: pylibmc error while storing collection timestamps")
                log(e)

    
    def _dict_cache_key(self, collection):
        return "nntsc_%s_%s" % (self.nntscid, str(collection))

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
