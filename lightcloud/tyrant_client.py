import threading

import pytyrant


#--- Connection manager ----------------------------------------------
connections = threading.local()

def get_connection(host, port):
    key = '%s:%s' % (host, port)
    cur_connection = hasattr(connections, key)

    if not cur_connection:
        client = pytyrant.Tyrant.open(host, port)
        client.cache_key = key
        setattr(connections, key, client)

    return getattr(connections, key)

def close_open_connections():
    for key in dir(connections):
        if '__' not in key:
            getattr(connections, key).close()
            delattr(connections, key)


#--- pytyrant client ----------------------------------------------
class TyrantClient:

    def __init__(self, servers):
        to_int = lambda item: (item[0], int(item[1]))
        self.servers = [ to_int(s.split(':')) for s in servers ]

    #--- Incr and decr ----------------------------------------------
    def incr(self, key, delta=1):
        return self.call_db(key, 'call_func',
                            'incr', key, "%s" % delta, record_locking=True)

    #--- Set, get and delete ----------------------------------------------
    def set(self, key, val, **kw):
        try:
            self.call_db(key, 'put',
                         key, val)
            return True
        except:
            return False

    def get(self, key, **kw):
        try:
            val = self.call_db(key, 'get', key)
        except Exception, e:
            return None
        return val

    def delete(self, key):
        try:
            self.call_db(key, 'out', key)
            return True
        except:
            return False

    #--- List ----------------------------------------------
    def _encode_list(self, values):
        v_encoded = []
        for v in values:
            v_encoded.append('%s~' % v)
        return ''.join(v_encoded)

    def list_add(self, key, values):
        return self.call_db(key, 'call_func',
                            'list_add', key,
                            self._encode_list(values), record_locking=True)

    def list_remove(self, key, values):
        return self.call_db(key, 'call_func',
                            'list_remove', key,
                            self._encode_list(values), record_locking=True)

    #--- db man ----------------------------------------------
    def call_db(self, key, operation, *k, **kw):
        key_hash = hash(key)

        def pick_server(skey, servers):
            index = key_hash % len(servers)
            host, port = servers.pop(index)
            return host, port

        servers = list(self.servers)

        while len(servers) > 0:
            host, port = pick_server(key, servers)

            try:
                db = get_connection(host, port)
                return getattr(db, operation)(*k, **kw)
            except Exception, e:
                continue

        raise

class TyrantNode(TyrantClient):
    """Extends the tyrant client with a proper __str__ method"""

    def __init__(self, name, nodes, *k, **kw):
        self.name = name
        TyrantClient.__init__(self, nodes, *k, **kw)

    def __str__(self):
        return self.name
