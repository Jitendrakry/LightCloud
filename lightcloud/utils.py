import base64
import zlib
import pickle
import random
from StringIO import StringIO

import memcache
import pytyrant


class TyrantClient:

    def __init__(self, servers):
        to_int = lambda item: (item[0], int(item[1]))
        self.servers = [ to_int(s.split(':')) for s in servers ]

    #--- Incr and decr ----------------------------------------------
    def incr(self, key, delta=1):
        db = self.get_db(key)
        try:
            return db.call_func('incr', key, "%s" % delta, record_locking=True)
        finally:
            db.close()

    #--- Set, get and delete ----------------------------------------------
    def set(self, key, val, **kw):
        db = self.get_db(key)

        try:
            db.put(key, val)
            return True
        except:
            return False

        finally:
            db.close()

    def get(self, key, **kw):
        db = self.get_db(key)
        try:
            try:
                val = db.get(key)
            except:
                return None
            return val
        finally:
            db.close()

    def delete(self, key):
        db = self.get_db(key)

        try:
            db.out(key)
            return True
        except:
            return False

        finally:
            db.close()

    #--- List ----------------------------------------------
    def _encode_list(self, values):
        v_encoded = []
        for v in values:
            v_encoded.append('%s~' % v)
        return ''.join(v_encoded)

    def list_add(self, key, values):
        db = self.get_db(key)
        try:
            return db.call_func('list_add', key, self._encode_list(values), record_locking=True)
        finally:
            db.close()

    def list_remove(self, key, values):
        db = self.get_db(key)
        try:
            return db.call_func('list_remove', key, self._encode_list(values), record_locking=True)
        finally:
            db.close()

    def get_db(self, key):
        servers = list(self.servers)

        #Load balance by key
        index = hash(key) % len(servers)
        first_host, first_port = servers.pop(index)
        try:
            return pytyrant.Tyrant.open(first_host, first_port)
        except:
            pass

        #This did not work out, try the other servers
        for host, port in servers:
            try:
                return pytyrant.Tyrant.open(host, port)
            except Exception, e:
                if e[0] == 61:
                    continue
                raise
        raise


class MemcacheClient(memcache.Client):
    """Memcache client to talk to Tyrant.
    """
    def __init__(self, *k, **kw):
        memcache.Client.__init__(self, *k, **kw)

    def get(self, key, **kw):
        key = base64.b64encode(key)
        val = memcache.Client.get(self, key, **kw)
        return val

    def delete(self, key):
        key = base64.b64encode(key)
        return memcache.Client.delete(self, key)

    def incr(self, key, delta=1):
        key = base64.b64encode(key)
        return memcache.Client.incr(self, key, delta=delta)

    def decr(self, key, delta=1):
        key = base64.b64encode(key)
        return memcache.Client.decr(self, key, delta=delta)

    def set(self, key, val, **kw):
        key = base64.b64encode(key)
        result = memcache.Client.set(self, key, val, **kw)
        return result
