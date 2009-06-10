from redis import get_connection, close_open_connections

class RedisClient:

    def __init__(self, servers):
        to_int = lambda item: (item[0], int(item[1]))
        self.servers = [ to_int(s.split(':')) for s in servers ]

    #--- Incr and decr ----------------------------------------------
    def incr(self, key, delta=1):
        if delta > 0:
            return self.call_db(key, 'incr',
                                key, delta)
        else:
            return self.call_db(key, 'decr',
                                key, delta*-1)

    #--- Set, get and delete ----------------------------------------------
    def set(self, key, val, **kw):
        try:
            return self.call_db(key, 'set', key, val)
        except:
            return False

    def get(self, key, **kw):
        try:
            return self.call_db(key, 'get', key)
        except Exception, e:
            return None
        return val

    def delete(self, key):
        try:
            return self.call_db(key, 'delete', key)
        except Exception, e:
            return False

    #--- List ----------------------------------------------
    def list_add(self, key, values, limit=200):
        for val in values:
            self.call_db(key, 'push', key, val)
        self.call_db(key, 'ltrim', key, 0, limit)
        return True

    def list_remove(self, key, values):
        for val in values:
            self.call_db(key, 'lrem', key, val)
        return True

    def list_get(self, key, start=0, end=200):
        return self.call_db(key, 'lrange', key, start, end)

    #--- db man ----------------------------------------------
    def call_db(self, key, operation, *k, **kw):
        key_hash = hash(key)

        def pick_server(skey, servers):
            index = key_hash % len(servers)
            host, port = servers.pop(index)
            return host, port

        servers = list(self.servers)

        exp = Exception("unknown")

        while len(servers) > 0:
            host, port = pick_server(key, servers)

            try:
                db = get_connection(host, port)
                return getattr(db, operation)(*k, **kw)
            except Exception, e:
                print e
                exp = e
                continue

        raise exp

class RedisNode(RedisClient):

    def __init__(self, name, nodes, *k, **kw):
        self.name = name
        RedisClient.__init__(self, nodes, *k, **kw)

    def __str__(self):
        return self.name
