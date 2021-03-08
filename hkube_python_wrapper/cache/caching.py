import datetime
import threading
from pympler import asizeof
import hkube_python_wrapper.util.type_check as typeCheck
from hkube_python_wrapper.util.decorators import timing
from hkube_python_wrapper.util.logger import log

MB = 1024 * 1024


class Cache:
    def __init__(self, config):
        self._cache = dict()
        self._maxCacheSize = config.get('maxCacheSize') * MB
        self.sumSize = 0
        self.lock = threading.Lock()
    @timing
    def update(self, key, value, size=None, header=None):
        if (size is None):
            if (typeCheck.isBytearray(value)):
                size = len(value)
            else:
                size = asizeof.asizeof(value)
        if (key in self._cache):
            return True
        if (size > self._maxCacheSize):
            log.warning("unable to insert cache value of size {size} MB, max: ({max}) MB", size=size, max=self._maxCacheSize)
            return False
        while ((self.sumSize + size) > self._maxCacheSize):
            self._remove_oldest()
        with self.lock:
            self._cache[key] = {'timestamp': datetime.datetime.now(), 'size': size, 'value': value, 'header': header}
        self.sumSize += size
        return True

    def __contains__(self, key):
        return key in self._cache

    def _remove_oldest(self):
        with self.lock:
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
