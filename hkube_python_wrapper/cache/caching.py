import datetime
import msgpack


class CustomCache:
    def __init__(self, config):
        self._cache = dict()
        self._maxCacheSize = config.get('maxCacheSize')
        self.sumSize = 0

    def update(self, key, value):
        size = len(msgpack.packb(value))
        if (key in self._cache):
            return
        while (self.sumSize + size) >= self._maxCacheSize*1000*1000:
            if (len(self._cache.keys()) == 0):
                print("Trying to insert a value of size " + str(size) + " bytes, larger than " + str(self._maxCacheSize) + "MB")
                return
            else:
                self._remove_oldest()
        self._cache[key] = {'timestamp': datetime.datetime.now(), 'size': size, 'value': value}
        self.sumSize += size
        return key

    def __contains__(self, key):
        return key in self._cache

    def _remove_oldest(self):
        oldest = None
        for key in self._cache:
            if oldest is None:
                oldest = key
            elif self._cache[key]['timestamp'] < self._cache[oldest]['timestamp']:
                oldest = key
        self.sumSize -= self._cache[key]['size']
        self._cache.pop(oldest)

    def get(self, key):
        item = self._cache[key]
        return item.get('value')
