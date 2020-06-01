import datetime

class CustomCache:
    def __init__(self, config):
        self._cache = dict()
        self._maxCacheSize = config.get('maxCacheSize')

    def update(self, key, value):
        if key not in self._cache and len(self._cache) >= self._maxCacheSize:
            self._remove_oldest()
        self._cache[key] = {'timestamp': datetime.datetime.now(), 'value': value}

    def __contains__(self, key):
        return key in self._cache

    def _remove_oldest(self):
        oldest = None
        for key in self._cache:
            if oldest is None:
                oldest = key
            elif self._cache[key]['timestamp'] < self._cache[oldest]['timestamp']:
                oldest = key
        self._cache.pop(oldest)

    def get(self, key):
        item = self._cache[key]
        return item.get('value')
        