from time import mktime
from datetime import datetime, timedelta

import lightcloud

class LimitExceeded(Exception):
    pass


def register_offend(key, extra_data='', system='default'):
    """Register an offend for `key`. `data` should be an integer or a string.
    """
    now = get_unixtime(datetime.utcnow())

    if extra_data:
        current_offends = get_offends(key, system)
        #Don't count duplicates
        for offend in current_offends:
            if offend[1] == extra_data:
                return

    lightcloud.list_add('offends_%s' % key,
                        ['%s///%s' % (now, extra_data)],
                        system=system)


def get_offends(key, system='default'):
    """Returns a list of offends that are registreted for `key`.

    The return structure is [(date1, extra_data1), ..., (dateN, extra_dataN)]
    """
    result = []
    ll = lightcloud.list_get('offends_%s' % key, system=system)
    for item in ll:
        sp = item.split('///')
        result.append( (get_datetime(float(sp[0])), sp[1]) )
    return result


def reset_offends(key, system='default'):
    lightcloud.list_varnish('offends_%s' % key, system=system)


def analyze_offends(list_of_offends, limit, hours=48):
    """Raises `LimitExceeded` if list_of_offends exceedes the limit.

    `hours` specifies at what interval one looks, e.g. if `hours` is
    48, then only the offends registred in the last 48 horus will be looked at.
    """
    x_ago = datetime.utcnow() - timedelta(hours=48)

    count = 0
    for offend in list_of_offends:
        if offend[0] > x_ago:
            count += 1

    if count > limit:
        raise LimitExceeded()


#--- Helpers ----------------------------------------------
def get_unixtime(datetime_obj):
    return mktime(datetime_obj.timetuple()) + 1e-6*datetime_obj.microsecond

def get_datetime(unix_time):
    return datetime.fromtimestamp(unix_time)
