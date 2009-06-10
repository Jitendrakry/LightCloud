#--- Setup path ----------------------------------------------
from time import time
import sys
from os.path import realpath, pardir, dirname, join
sys.path.insert(0, realpath(join(dirname(__file__), '..', '..')))

import lightcloud

#--- Setup clouds ----------------------------------------------
LIGHT_CLOUD = {
    'lookup1_A': [ '127.0.0.1:10000' ],
    'storage1_A': [ '127.0.0.1:12000']
}
lookup_nodes, storage_nodes = lightcloud.generate_nodes(LIGHT_CLOUD)
lightcloud.init(lookup_nodes, storage_nodes, node_type=lightcloud.RedisNode, system='redis')


LIGHT_CLOUD = {
    'lookup1_A': [ '127.0.0.1:41201', '127.0.0.1:51201' ],
    'storage1_A': [ '127.0.0.1:44201', '127.0.0.1:54201' ]
}
lookup_nodes, storage_nodes = lightcloud.generate_nodes(LIGHT_CLOUD)
lightcloud.init(lookup_nodes, storage_nodes, node_type=lightcloud.TyrantNode, system='tyrant')


#--- Run the tests ----------------------------------------------
def generic_bench(name, times_run, fn):
    start = time()
    for i in range(0, times_run):
        fn()
    end = time()-start
    pr_sec = float(times_run)/end
    print 'Finished "%s" %s times in %0.2f sec [%0.1f operations pr.sec]' %\
            (name, times_run, end, pr_sec)


#--- Support ----------------------------------------------
generic_bench('Tyrant set', 10000,
              lambda: lightcloud.set('hello', 'world', system='tyrant'))
generic_bench('Redis set', 10000,
              lambda: lightcloud.set('hello', 'world', system='redis'))

print '------'

generic_bench('Tyrant get', 10000,
              lambda: lightcloud.get('hello', system='tyrant'))
generic_bench('Redis get', 10000,
              lambda: lightcloud.get('hello', system='redis'))

print '------'

generic_bench('Tyrant list_add', 10000,
              lambda: lightcloud.list_add('hello_l', ['1'], system='tyrant'))
generic_bench('Redis list_add', 10000,
              lambda: lightcloud.list_add('hello_l', ['1'], system='redis'))

print '------'

generic_bench('Tyrant delete', 10000,
              lambda: lightcloud.delete('hello', system='tyrant'))
generic_bench('Redis delete', 10000,
              lambda: lightcloud.delete('hello', system='redis'))
