from datetime import datetime, timedelta
from time import gmtime, strftime

import lightcloud


#--- Simple counter ----------------------------------------------
def update_counter(key, delta=1, system='default'):
    return lightcloud.incr(_key(key), delta=delta, system=system)

def get_counts(key, system='default'):
    count = lightcloud.get(_key(key), system=system)
    if count:
        return long(count)
    return 0

def reset_counter(key, system='default'):
    return lightcloud.delete(_key(key))



#--- Day counter ----------------------------------------------
def update_day_counter(key, delta=1, system='default'):
    now = _get_now()
    day_key = '%s_%s' % (_key(key), _format_date(now))
    return lightcloud.incr(day_key, delta=delta, system=system)

def get_day_counts(key, offset=None, limit=10, system='default'):
    if not offset:
        offset = _get_now()

    result = []
    for i in xrange(0, limit):
        day_key = '%s_%s' % (_key(key), _format_date(offset))

        count = lightcloud.get(day_key) or 0
        if count:
            count = long(count)

        result.append({
            'date': datetime(*offset.timetuple()[0:7]),
            'counts': count
        })

        offset = _previous_day(offset)

    return result

def reset_day_counter(key, days=10, system='default'):
    date = _get_now()

    for i in xrange(0, days):
        day_key = '%s_%s' % (_key(key), _format_date(date))
        lightcloud.delete(day_key)
        date = _previous_day(date)

    return True


#--- Helpers ----------------------------------------------
def _get_now():
    return datetime.utcnow()

def _key(key):
    return '_LCC.%s' % key

def _format_date(date):
    return strftime("%Y-%m-%d", date.timetuple())

def _previous_day(date):
    return date - timedelta(days=1)
