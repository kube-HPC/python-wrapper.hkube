from hkube_python_wrapper.cache.caching import Cache


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
    test_get_all()
