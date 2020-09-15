import datetime
from pympler import asizeof
import hkube_python_wrapper.util.type_check as typeCheck
from hkube_python_wrapper.util.decorators import timing


class Cache:
    def __init__(self, config):
        self._cache = dict()
        self._maxCacheSize = config.get('maxCacheSize')
        self.sumSize = 0

    @timing
    def update(self, key, value, size=None, header=None):
        if (size is None):
            if (typeCheck.isBytearray(value)):
                size = len(value)
            else:
                size = asizeof.asizeof(value)
        if (key in self._cache):
            return key
        while (self.sumSize + size) >= self._maxCacheSize * 1000 * 1000:
            if not (self._cache.keys()):
                print("Trying to insert a value of size " + str(size) + " bytes, larger than " + str(
                    self._maxCacheSize) + "MB")
                return None
            self._remove_oldest()
        self._cache[key] = {'timestamp': datetime.datetime.now(), 'size': size, 'value': value, 'header': header}
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
        self.sumSize -= self._cache[oldest]['size']
        self._cache.pop(oldest)

    def get(self, key):
        item = self._cache.get(key)
        if (item is not None):
            return item.get('value')
        return None

    def getWithHeader(self, key):
        item = self._cache.get(key)
        if (item is not None):
            return item.get('header'), item.get('value')
        return None

    def getAll(self, keys):
        tasksNotInCache = []
        valuesInCache = []
        for key in keys:
            cacheRecord = self._cache.get(key)
            if (cacheRecord):
                valuesInCache.append(cacheRecord.get('value'))
            else:
                tasksNotInCache.append(key)
        return tasksNotInCache, valuesInCache
