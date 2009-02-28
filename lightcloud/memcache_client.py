#--- Memcached client ----------------------------------------------
import memcache

class MemcacheClient(memcache.Client):
    """Memcache client to talk to Tyrant.
    """
    def __init__(self, *k, **kw):
        memcache.Client.__init__(self, *k, **kw)

    def get(self, key, **kw):
        val = memcache.Client.get(self, key, **kw)
        return val

    def delete(self, key):
        return memcache.Client.delete(self, key)

    def incr(self, key, delta=1):
        return memcache.Client.incr(self, key, delta=delta)

    def decr(self, key, delta=1):
        return memcache.Client.decr(self, key, delta=delta)

    def set(self, key, val, **kw):
        result = memcache.Client.set(self, key, val, **kw)
        return result
