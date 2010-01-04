from threading import local

class LocalCache:

    def __init__(self):
        self.cache = local()

    def get(self, key):
        if hasattr(self.cache, key):
            return getattr(self.cache, key)
        return None

    def set(self, key, value):
        setattr(self.cache, key, value)

    def delete(self, key):
        if hasattr(self.cache, key):
            delattr(self.cache, key)

LOCAL_CACHE = None
def get_local_cache():
    global LOCAL_CACHE
    if not LOCAL_CACHE:
        LOCAL_CACHE = LocalCache()
    return LOCAL_CACHE
