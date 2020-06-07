

from hkube_python_wrapper.util.object_path import getPath, setPath
import hkube_python_wrapper.util.type_check as typeCheck


def test_getPath_no_path():
    obj = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    path = 'green.no.such'
    result = getPath(obj, path)
    assert result == 'DEFAULT'


def test_getPath_array():
    obj = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    path = '5'
    result = getPath(obj, path)
    assert typeCheck.isInt(result)


def test_getPath_nested_array():
    obj = {"green": {"prop": [1, 2, 3, 4, 5, 6, 7, 8, 9]}}
    path = 'green.prop'
    result = getPath(obj, path)
    assert typeCheck.isList(result)


def test_getPath_nested_array_index():
    obj = {"green": {"prop": [1, 2, 3, 4, 5, 6, 7, 8, 9]}}
    path = 'green.prop.5'
    result = getPath(obj, path)
    assert typeCheck.isInt(result)


def test_getPath_bytes():
    sizeBytes = 10 * 1000000
    obj = {"prop": [{"bytesArr": bytearray(sizeBytes)}]}
    path = 'prop.0.bytesArr'
    result = getPath(obj, path)
    assert result != 'DEFAULT'


def test_setPath_bytes():
    inputArgs = [1, False, {"prop": "$$guid-1234"}]
    value = bytearray(10 * 1000000)
    path = '2.prop'
    setPath(inputArgs, path, value)
    assert typeCheck.isBytearray(inputArgs[2]["prop"])


def test_setPath_array():
    inputArgs = [1, False, {"prop": "$$guid-1234"}]
    value = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    path = '2.prop'
    setPath(inputArgs, path, value)
    assert typeCheck.isList(inputArgs[2]["prop"])
