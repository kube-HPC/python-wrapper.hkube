
import collections
from util.object_path import getPath, setPath


def test_getPath_no_path():
    obj = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    path = 'green.no.such'
    result = getPath(obj, path)
    assert result == 'DEFAULT'


def test_getPath_array():
    obj = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    path = '5'
    result = getPath(obj, path)
    assert isinstance(result, int)


def test_getPath_nested_array():
    obj = {"green": {"prop": [1, 2, 3, 4, 5, 6, 7, 8, 9]}}
    path = 'green.prop'
    result = getPath(obj, path)
    assert isinstance(result, collections.Sequence)


def test_getPath_nested_array_index():
    obj = {"green": {"prop": [1, 2, 3, 4, 5, 6, 7, 8, 9]}}
    path = 'green.prop.5'
    result = getPath(obj, path)
    assert isinstance(result, int)


def test_getPath_bytes():
    sizeBytes = 10 * 1000000
    obj = {"prop": [{"bytesArr": bytearray(sizeBytes)}]}
    path = 'prop.0.bytesArr'
    result = getPath(obj, path)
    assert result != 'DEFAULT'


def test_setPath_bytes():
    result = [1, False, {"prop": "bla"}]
    obj = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    path = '2/prop'
    setPath(result, path, obj)
    assert isinstance(result[2]["prop"], collections.Sequence)
