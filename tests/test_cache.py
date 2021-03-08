from hkube_python_wrapper.cache.caching import Cache
import concurrent.futures
import datetime

def test_reaching_limit():
    cache = Cache({"maxCacheSize": 4})
    value1 = {"bytes": bytearray(1000000)}
    cache.update("task1", value1, 1000000)
    assert len(cache._cache) == 1
    cache.update("task2", value1, 1000000)
    cache.update("task3", value1, 1000000)
    assert len(cache._cache) == 3
    cache.update("task4", value1, 1500000)
    assert len(cache._cache) == 3
    assert "task4" in cache
    assert "task1" not in cache

def test_threading():
    cache = Cache({"maxCacheSize": 4})
    # fill cache
    startTime=datetime.datetime.now()
    for i in range(10000):
        values=(str(i),{"bytes": bytearray(500)},500)
        cache._cache[values[0]] = {'timestamp': startTime+datetime.timedelta(0,1), 'size': values[2], 'value': values[1], 'header': None}
    request=[]
    for i in range(1000):
        request.append(('key'+str(i),{"bytes": bytearray(50000)},50000))
    res=[]
    def process(item):
        cache.update(item[0], item[1])
        return item[0]
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for out in executor.map(process,request):
            res.append(out)
    assert len(res) == 1000

def test_too_large_message():
    cache = Cache({"maxCacheSize": 5})
    value1 = {"bytes": bytearray(1000000)}
    value2 = {"bytes": bytearray(5000000)}
    result = cache.update("task1", value1, 1000000)
    assert result == True
    assert len(cache._cache) == 1
    result = cache.update("task2", value2, 6000000)
    assert result == False
    assert len(cache._cache) == 1


def test_get_all():
    cache = Cache({"maxCacheSize": 5})
    value1 = {"data": "1"}
    value2 = {"data": "2"}
    result = cache.update("task1", value1)
    assert result == True
    assert len(cache._cache) == 1
    cache.update("task2", value2)
    assert len(cache._cache) == 2
    tasksNotInCache, valuesInCache = cache.getAll(['task1', 'task2', 'task3', 'task4'])
    assert len(tasksNotInCache) == 2
    assert "task3" in set(tasksNotInCache)
    assert "task4" in set(tasksNotInCache)
    assert len(valuesInCache) == 2
    data = list(map(lambda value: value.get('data'), valuesInCache))
    assert "1" in set(data)
    assert "2" in set(data)

if __name__ == '__main__':
    test_threading()