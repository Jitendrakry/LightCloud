import threading
import types

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

def close_connection(key):
    if hasattr(connections, key):
        getattr(connections, key).close()
        delattr(connections, key)

def close_open_connections():
    for key in dir(connections):
        if '__' not in key:
            close_connection(key)


#--- pytyrant client ----------------------------------------------
class TyrantClient:

    def __init__(self, servers):
        to_int = lambda item: (item[0], int(item[1]))
        self.servers = [ to_int(s.split(':')) for s in servers ]

    #--- Incr and decr ----------------------------------------------
    def incr(self, key, delta=1):
        key = encode_key(key)
        return self.call_db(key, 'ext',
                            'incr', 0, key, "%s" % delta)

    #--- Set, get and delete ----------------------------------------------
    def set(self, key, val, **kw):
        key = encode_key(key)

        try:
            self.call_db(key, 'put',
                         key, val)
            return True
        except Exception, e:
            return False

    def get(self, key, **kw):
        key = encode_key(key)

        try:
            val = self.call_db(key, 'get', key)
        except Exception, e:
            return None
        return val

    def delete(self, key):
        key = encode_key(key)

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

    def list_init(self, key):
        key = encode_key(key)
        self.set(key, '')

    def list_add(self, key, values, limit=200):
        key = encode_key(key)
        if limit != 200:
            key = '%s|%s' % (limit, key)
        return self.call_db(key, 'ext',
                            'list_add', 0,
                            key, self._encode_list(values))

    def list_set(self, key, values):
        key = encode_key(key)
        values = self._encode_list(values)
        return self.set(key, values)

    def list_remove(self, key, values):
        key = encode_key(key)
        return self.call_db(key, 'ext',
                            'list_remove', 0,
                            key, self._encode_list(values))

    def list_get(self, key):
        key = encode_key(key)
        value = self.get(key)
        if value:
            return [ v for v in value.split(r'~') if v ]
        return []

    #--- db man ----------------------------------------------
    def call_db(self, key, operation, *k, **kw):
        key_hash = hash(key)

        def pick_server(skey, servers):
            index = key_hash % len(servers)
            host, port = servers.pop(index)
            return host, port

        servers = list(self.servers)

        exp = Exception("unknown")

        tried_servers = {}

        while len(servers) > 0:
            host, port = pick_server(key, servers)

            try:
                db = get_connection(host, port)
                return getattr(db, operation)(*k, **kw)
            except Exception, e:
                server_key = '%s:%s' % (host, port)

                if not server_key in tried_servers:
                    close_connection(server_key)
                    servers.append( (host, port) )
                    tried_servers[server_key] = True
                    continue

                exp = e
                continue

        raise exp


class TyrantNode(TyrantClient):
    """Extends the tyrant client with a proper __str__ method"""

    def __init__(self, name, nodes, *k, **kw):
        self.name = name
        TyrantClient.__init__(self, nodes, *k, **kw)

    def __str__(self):
        return self.name


def encode_key(key):
    if type(key) == types.UnicodeType:
        key = key.encode('utf8')
    return key
