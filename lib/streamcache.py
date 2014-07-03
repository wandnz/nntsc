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

    def store_timestamps(self, collection, streamid, last, first=None):
        if first == None and last == None:
            return
        
        coldict = self._fetch_dict(collection)

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

        self._store_dict(collection, coldict)

    def store_timestamp_dict(self, collection, tsdict):
        self._store_dict(collection, tsdict)

    def fetch_timestamp_dict(self, collection):
        return self._fetch_dict(collection)

    def fetch_timestamps(self, collection, streamid):

        coldict = self._fetch_dict(collection)
        if streamid not in coldict:
            return (None, None)
        
        return coldict[streamid]
        
    def _fetch_dict(self, collection):
        key = self._dict_cache_key(collection)
       
        #print "Fetching using key", key 
        with self.mcpool.reserve() as mc:
            try:
                if key in mc:
                    return mc.get(key)
            except pylibmc.SomeErrors as e:
                log("Warning: pylibmc error while fetching collection timestamps")
                log(e)
        return {}

    def _store_dict(self, collection, coldict):
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
