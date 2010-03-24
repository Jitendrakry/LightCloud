from hash_ring import HashRing

try:
    from memcache_client import MemcachedNode
except ImportError, e:
    pass

from tyrant_client import TyrantNode, close_open_connections as tryant_close, get_connection
from redis_client import RedisNode, close_open_connections as redis_close, get_connection

from local_cache import get_local_cache


#--- Global ----------------------------------------------
LOOKUP_LOOKS = 2

USE_CACHE = True
GET_CACHE = get_local_cache

SYSTEMS = {}

default_node = TyrantNode


#--- Init and config ----------------------------------------------
def init(lookup_nodes, storage_nodes, system='default', node_type=None):
    name_to_s_node = {}
    name_to_l_node = {}

    lookup_ring = generate_ring(lookup_nodes, name_to_l_node, node_type)
    storage_ring = generate_ring(storage_nodes, name_to_s_node, node_type)

    SYSTEMS[system] = (lookup_ring, storage_ring,
                       name_to_l_node, name_to_s_node)


def generate_nodes(lc_config):
    lookup_nodes = {}
    storage_nodes = {}

    for k, v in lc_config.items():
        if 'lookup' in k:
            lookup_nodes[k] = v
        elif 'storage' in k:
            storage_nodes[k] = v

    return lookup_nodes, storage_nodes


#--- System accessors ----------------------------------------------
def get_lookup_ring(system='default'):
    return SYSTEMS[system][0]

def get_storage_ring(system='default'):
    return SYSTEMS[system][1]


#--- Node accessors ----------------------------------------------
def storage_nodes(system='default'):
    """Returns the storage nodes in the storage ring"""
    for node in SYSTEMS[system][3].values():
        yield node

def lookup_nodes(system='default'):
    """Returns the lookup nodes in the lookup ring"""
    for node in SYSTEMS[system][2].values():
        yield node

def get_storage_node(name, system='default'):
    """Returns a storage node by its name"""
    return SYSTEMS[system][3].get(name)

def get_lookup_node(name, system='default'):
    """Returns a lookup node by its name"""
    return SYSTEMS[system][2].get(name)


#--- Operations ----------------------------------------------
def incr(key, delta=1, system='default'):
    storage_node = locate_node_or_init(key, system)
    cache_delete(key, system)
    return storage_node.incr(key, delta)


#--- List ----------------------------------------------
def list_init(key, system='default'):
    key = 'll_%s' % key
    storage_node = locate_node_or_init(key, system)
    cache_delete(key, system)
    return storage_node.list_init(key)

def list_get(key, system='default', **kw):
    key = 'll_%s' % key

    if USE_CACHE:
        val = cache_get(key, system)

        if val:
            return val

    storage_node = locate_node_or_init(key, system)
    val = storage_node.list_get(key, **kw)

    if USE_CACHE:
        cache_set(key, val, system=system)

    return val

def list_add(key, values, system='default', limit=200):
    key = 'll_%s' % key
    storage_node = locate_node_or_init(key, system)
    result = storage_node.list_add(key, values, limit)
    cache_delete(key, system)
    return result

def list_set(key, values, system='default'):
    key = 'll_%s' % key
    storage_node = locate_node_or_init(key, system)
    result = storage_node.list_set(key, values)
    cache_delete(key, system)
    return result

def list_remove(key, values, system='default'):
    key = 'll_%s' % key
    storage_node = locate_node_or_init(key, system)
    result = storage_node.list_remove(key, values)
    cache_delete(key, system)
    return result

def list_varnish(key, system='default'):
    key = 'll_%s' % key
    result = delete(key, system)
    cache_delete(key, system)
    return result

def list_is_created(key, system='default'):
    key = 'll_%s' % key
    value = get(key, system)
    if value:
        return True
    return False


#--- Get, set and delete ----------------------------------------------
def get(key, system='default', enable_cache=True):
    """Lookup's the storage node in the
    lookup ring and return's the stored value
    """
    if enable_cache:
        if USE_CACHE:
            value = cache_get(key, system)
            if value:
                return value

    #Try to look it up directly
    result = None

    storage_node = get_storage_ring(system).get_node(key)
    value = storage_node.get(key)

    if value != None:
        result = value

    #Else use the lookup ring to locate the key
    if result == None:
        storage_node = locate_node(key, system)

        if storage_node:
            result = storage_node.get(key)

    if USE_CACHE:
        cache_set(key, result, system=system)

    return result


def delete(key, system='default'):
    """Lookup's the storage node in the lookup ring
    and deletes the key from lookup ring and storage node
    """
    for i, lookup_node in enumerate(get_lookup_ring(system).iterate_nodes(key)):
        if i > 0:
            break
        lookup_node.delete(key)

    for i, storage_node in enumerate(get_storage_ring(system).iterate_nodes(key)):
        if i > 0:
            break
        storage_node.delete(key)

    cache_delete(key, system)

    return True


def set(key, value, system='default'):
    """Looks in the lookup ring to see if the key is stored,

    If it is, same storage node is used.  If it isn't,
    then the storage node is determinted by using hash_ring.
    """
    storage_node = locate_node_or_init(key, system)

    if USE_CACHE:
        cache_set(key, value, system=system)

    return storage_node.set(key, value)


#--- Lookup cloud ----------------------------------------------
def locate_node_or_init(key, system):
    storage_node = locate_node(key, system)

    if not storage_node:
        storage_node = get_storage_ring(system).get_node(key)

        lookup_node = get_lookup_ring(system).get_node(key)
        lookup_node.set(key, str(storage_node))

    return storage_node


def locate_node(key, system='default'):
    """Locates a node in the lookup ring, returning its storage node
    or `None` if the storage node isn't found.
    """
    nodes = get_lookup_ring(system).iterate_nodes(key)
    for lookups, node in enumerate(nodes):
        if lookups >= LOOKUP_LOOKS:
            return None

        value = node.get(key)
        if value:
            break

    if not value:
        return None

    if lookups == 0:
        return get_storage_node(value, system)
    else:
        return _clean_up_ring(key, value, system)


def _clean_up_ring(key, value, system):
    """If the number of hops is greater than 0,
    then we clean up the ring by setting the right value,
    and deleting the key from the older nodes"""
    nodes = get_lookup_ring(system).iterate_nodes(key)

    for i, node in enumerate(nodes):
        if i > 1:
            break

        if i == 0:
            node.set(key, value)
        else:
            node.delete(key)

    return get_storage_node(value, system)


#--- Cache layer ----------------------------------------------
def cache_set(key, value, system='default', time=0):
    key = '%s:%s' % (system, key)
    return GET_CACHE().set(key, value, time=time)

def cache_get(key, system='default'):
    key = '%s:%s' % (system, key)
    return GET_CACHE().get(key)

def cache_delete(key, system='default'):
    key = '%s:%s' % (system, key)
    return GET_CACHE().delete(key)

def expire_cache():
    pass


#--- Helpers ----------------------------------------------
def close_open_connections():
    """Closes open connections to nodes"""
    tryant_close()
    redis_close()

def generate_ring(nodes, name_to_obj, node_type=None):
    """Given a set of nodes it created nodes's
    and returns a hash ring with them"""

    global default_node
    if not node_type:
        node_type = default_node

    objects = []

    for name in nodes:
        obj = node_type(name, nodes[name])
        name_to_obj[name] = obj
        objects.append( obj )

    return HashRing(objects)


def regenerate_ring(system='default'):
    try:
        for node in lookup_nodes(system):
            node.disconnect_all()
        for node in storage_nodes(system):
            node.disconnect_all()
    except Exception, e:
        pass

    init(get_lookup_ring(system).nodes, get_storage_ring(system).nodes, system)
