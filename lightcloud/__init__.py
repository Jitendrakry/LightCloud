from hash_ring import HashRing
from utils import MemcacheClient, TyrantClient,\
                  close_open_connections, get_connection
from threading import local

local_cache = local()

#--- Global and init ----------------------------------------------
storage_ring = None
lookup_ring = None
default_node = None

name_to_s_node = {}
name_to_l_node = {}

_lookup_nodes = None
_storage_nodes = None

def init(lookup_nodes, storage_nodes):
    global _lookup_nodes, _storage_nodes
    _lookup_nodes = lookup_nodes
    _storage_nodes = storage_nodes

    global storage_ring, lookup_ring
    lookup_ring = generate_ring(lookup_nodes, name_to_l_node)
    storage_ring = generate_ring(storage_nodes, name_to_s_node)

def generate_nodes(lc_config):
    lookup_nodes = {}
    storage_nodes = {}

    for k, v in lc_config.items():
        if 'lookup' in k:
            lookup_nodes[k] = v
        elif 'storage' in k:
            storage_nodes[k] = v

    return lookup_nodes, storage_nodes


#--- Node accessors ----------------------------------------------
def storage_nodes():
    """Returns the storage nodes in the storage ring"""
    for node in name_to_s_node.values():
        yield node

def lookup_nodes():
    """Returns the lookup nodes in the lookup ring"""
    for node in name_to_l_node.values():
        yield node

def get_storage_node(name):
    """Returns a storage node by its name"""
    return name_to_s_node.get(name)

def get_lookup_node(name):
    """Returns a lookup node by its name"""
    return name_to_l_node.get(name)


#--- Accessors and mutators ----------------------------------------------
def incr(key, delta=1):
    storage_node = locate_node_or_init(key)
    return storage_node.incr(key, delta)


#--- List ----------------------------------------------
def list_init(key):
    key = 'll_%s' % key
    set(key, '')

def list_get(key):
    key = 'll_%s' % key
    value = get(key)
    if value:
        return [ v for v in value.split(r'~') if v ]
    return []

def list_add(key, values):
    key = 'll_%s' % key

    storage_node = locate_node_or_init(key)
    result = storage_node.list_add(key, values)

    if hasattr(local_cache, str(hash(key))):
        delattr(local_cache, str(hash(key)))
    return result

def list_remove(key, values):
    key = 'll_%s' % key

    storage_node = locate_node_or_init(key)
    result = storage_node.list_remove(key, values)

    if hasattr(local_cache, str(hash(key))):
        delattr(local_cache, str(hash(key)))

    return result

def list_varnish(key):
    key = 'll_%s' % key

    result = delete(key)
    if hasattr(local_cache, str(hash(key))):
        delattr(local_cache, str(hash(key)))
    return result


#--- Get, set and delete ----------------------------------------------
def get(key):
    """Lookup's the storage node in the
    lookup ring and return's the stored value
    """
    if hasattr(local_cache, str(hash(key))):
        return getattr(local_cache, str(hash(key)))

    #Try to look it up directly
    result = None

    storage_node = storage_ring.get_node(key)
    value = storage_node.get(key)
    if value != None:
        result = value

    #Else use the lookup ring to locate the key
    if result == None:
        storage_node = locate_node(key)

        if storage_node:
            result = storage_node.get(key)

    setattr(local_cache, str(hash(key)), result)

    if len(dir(local_cache)) > 750:
        clean_local_cache()

    return result

def delete(key):
    """Lookup's the storage node in the lookup ring
    and deletes the key from lookup ring and storage node
    """
    storage_node = locate_node(key)
    if not storage_node:
        storage_node = storage_ring.get_node(key)

    for i, lookup_node in enumerate(lookup_ring.iterate_nodes(key)):
        if i > 1:
            break
        lookup_node.delete(key)

    if storage_node:
        storage_node.delete(key)

    if hasattr(local_cache, str(hash(key))):
        delattr(local_cache, str(hash(key)))

    return True


def set(key, value):
    """Looks in the lookup ring to see if the key is stored,

    If it is, same storage node is used.  If it isn't,
    then the storage node is determinted by using hash_ring.
    """
    storage_node = locate_node_or_init(key)

    if hasattr(local_cache, str(hash(key))):
        delattr(local_cache, str(hash(key)))

    return storage_node.set(key, value)


#--- Lookup cloud ----------------------------------------------
def locate_node_or_init(key):
    storage_node = locate_node(key)

    if not storage_node:
        storage_node = storage_ring.get_node(key)

        lookup_node = lookup_ring.get_node(key)
        lookup_node.set(key, str(storage_node))

    return storage_node


def locate_node(key):
    """Locates a node in the lookup ring, returning its storage node
    or `None` if the storage node isn't found.
    """
    nodes = lookup_ring.iterate_nodes(key)
    for lookups, node in enumerate(nodes):
        if lookups > 2:
            return None

        value = node.get(key)
        if value:
            break

    if not value:
        return None

    if lookups == 0:
        return get_storage_node(value)
    else:
        return _clean_up_ring(key, value)

def _clean_up_ring(key, value):
    """If the number of hops is greater than 0,
    then we clean up the ring by setting the right value,
    and deleting the key from the older nodes"""
    nodes = lookup_ring.iterate_nodes(key)
    for i, node in enumerate(nodes):
        if i > 1:
            break

        if i == 0:
            node.set(key, value)
        else:
            node.delete(key)
    return get_storage_node(value)


#--- Helpers ----------------------------------------------
class MemcachedNode(MemcacheClient):
    """Extends the memcached client with a proper __str__ method"""

    def __init__(self, name, nodes, *k, **kw):
        self.name = name
        MemcacheClient.__init__(self, nodes, *k, **kw)

    def __str__(self):
        return self.name

class TyrantNode(TyrantClient):
    """Extends the tyrant client with a proper __str__ method"""

    def __init__(self, name, nodes, *k, **kw):
        self.name = name
        TyrantClient.__init__(self, nodes, *k, **kw)

    def __str__(self):
        return self.name

def generate_ring(nodes, name_to_obj):
    """Given a set of nodes it created nodes's
    and returns a hash ring with them"""
    global default_node
    if not default_node:
        default_node = TyrantNode

    objects = []

    for name in nodes:
        obj = default_node(name, nodes[name])
        name_to_obj[name] = obj
        objects.append( obj )

    return HashRing(objects)

def regenerate_ring():
    try:
        for node in lookup_nodes():
            node.disconnect_all()
        for node in storage_nodes():
            node.disconnect_all()
    except Exception, e:
        pass

    global storage_ring, lookup_ring
    lookup_ring = generate_ring(_lookup_nodes, name_to_l_node)
    storage_ring = generate_ring(_storage_nodes, name_to_s_node)

def clean_local_cache():
    keys = dir(local_cache)
    for k in keys:
        if '__' not in k:
            delattr(local_cache, k)
