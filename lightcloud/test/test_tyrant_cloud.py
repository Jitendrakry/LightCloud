#--- Setup path ----------------------------------------------
import sys
from os.path import realpath, pardir, dirname, join
sys.path.insert(0, realpath(join(dirname(__file__), '..', '..')))

import lightcloud

#--- Setup test nodes ----------------------------------------------
LIGHT_CLOUD = {
    'lookup1_A': [ '127.0.0.1:41201', '127.0.0.1:51201' ],
    'storage1_A': [ '127.0.0.1:44201', '127.0.0.1:54201' ]
}

lookup_nodes, storage_nodes = lightcloud.generate_nodes(LIGHT_CLOUD)
lightcloud.init(lookup_nodes, storage_nodes, node_type=lightcloud.TyrantNode)

def test_set_get():
    lightcloud.set('hello', 'world')
    assert lightcloud.get('hello') == 'world'

def test_delete():
    lightcloud.delete('hello')
    assert lightcloud.get('hello') == None

def test_incr():
    lightcloud.incr('hello', 2)
    assert lightcloud.get('hello') == '2'

    lightcloud.incr('hello', 2)
    assert lightcloud.get('hello') == '4'

def test_list():
    lightcloud.list_varnish('hello')

    lightcloud.list_add('hello', ['1', '2', '3'])
    lightcloud.list_add('hello', ['4'])
    assert lightcloud.list_get('hello') == ['4', '1', '2', '3']

    lightcloud.list_remove('hello', ['3'])
    assert lightcloud.list_get('hello') == ['4', '1', '2']

def test_limit_add():
    lightcloud.list_varnish('hello')

    for i in range(0, 250):
        lightcloud.list_add('hello', [i])
    assert len(lightcloud.list_get('hello')) == 200

    lightcloud.list_varnish('hello')
    for i in range(0, 100):
        lightcloud.list_add('hello', [i], limit=50)
    assert len(lightcloud.list_get('hello')) == 50

    cur = lightcloud.list_get('hello')
    i = 100
    for elm in cur:
        i -= 1
        assert elm == str(i)
