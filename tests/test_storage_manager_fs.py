import os
import shutil
import pytest
from hkube_python_wrapper.storage.storage_manager import StorageManager
from hkube_python_wrapper.util.encoding import Encoding
from tests.configs import config

def ensure_dir(dirName):
    if not os.path.exists(dirName):
        os.makedirs(dirName)  

config = config.storage
baseDirectory = config["fs"]["baseDirectory"]
rootDirectory = baseDirectory.split('/')[0]
sm = StorageManager(config)
encoding = Encoding(config['encoding'])

dir1 = 'dir1'
dir2 = 'dir2'

raw = {"data": 'all_my_data'}
(header, payload) = encoding.encode({"data": 'all_my_data'})


@pytest.fixture(scope="session", autouse=True)
def beforeall(request):
    ensure_dir(baseDirectory)

    def afterall():
        shutil.rmtree(rootDirectory, ignore_errors=True)

    request.addfinalizer(afterall)


def test_put_get_none():
    (h, p) = encoding.encode(None)
    options = {'path': dir1 + os.path.sep + 'a.txt', 'data': p}
    sm.storage.put(options)
    (head, payl) = sm.storage.get(options)
    decode = encoding.decode(header=head, value=payl)
    assert decode is None

def test_put_get():
    options = {'path': dir1 + os.path.sep + 'a.txt', 'data': payload}
    sm.storage.put(options)
    (head, payl) = sm.storage.get(options)
    decode = encoding.decode(header=head, value=payl)
    assert decode == raw

def test_multi_parts():
    obj = {
        "bytesData": bytearray(b'\xdd'*(5)),
        "anotherBytesData": bytearray(5),
        "string": 'stam_data',
        "int": 42
    }
    (header1, payload1) = encoding.encode(obj)
    options = {'path': dir1 + os.path.sep + 'a.txt', 'header': header1, 'data': payload1}
    sm.storage.put(options)
    (header2, payload2) = sm.storage.get(options)
    decoded = encoding.decode(header=header2, value=payload2)
    assert decoded == obj


def test_fail_to_get():
    options = {'path': dir1 + os.path.sep + 'no_such_path.txt', 'header': header, 'data': payload}
    with pytest.raises(Exception, match='Failed to read data from storage'):
        sm.storage.get(options)

def test_list():
    options = {'path': dir1 + os.path.sep + 'a.txt', 'header': header, 'data': payload}
    sm.storage.put(options)
    options = {'path': dir1 + os.path.sep + 'b.txt', 'header': header, 'data': payload}
    sm.storage.put(options)
    options = {'path': dir1 + os.path.sep + 'inner' + os.path.sep + 'c.txt', 'header': header, 'data': payload}
    sm.storage.put(options)
    options = {'path': dir1}
    resultArr = sm.storage.list(options)
    assert resultArr.__len__() == 3
    assert {'path': '/' + dir1 + '/a.txt'} in resultArr
    assert {'path': '/' + dir1 + '/b.txt'} in resultArr
    assert {'path': '/' + dir1 + '/inner/c.txt'} in resultArr


def test_prefixlist():
    options = {'path': dir1 + os.path.sep + 'a.txt', 'header': header, 'data': payload}
    sm.storage.put(options)
    options = {'path': dir1 + os.path.sep + 'b.txt', 'header': header, 'data': payload}
    sm.storage.put(options)
    options = {'path': dir1 + os.path.sep + 'inner' + os.path.sep + 'c.txt', 'header': header, 'data': payload}
    sm.storage.put(options)
    options = {'path': dir1}
    resultArr = sm.storage.listPrefix(options)
    assert resultArr.__len__() == 2
    assert '/' + dir1 + '/a.txt' in resultArr
    assert '/' + dir1 + '/b.txt' in resultArr
    assert '/' + dir1 + '/inner/c.txt' not in resultArr


def test_list_noneExsistingPath():
    options = {'path': 'noneExisting'}
    result = sm.storage.list(options)
    assert result == None


def test_delete():
    options = {'path': dir1 + os.path.sep + 'a.txt', 'header': header, 'data': payload}
    sm.storage.put(options)
    options = {'path': dir1 + os.path.sep + 'b.txt', 'header': header, 'data': payload}
    sm.storage.put(options)
    sm.storage.delete(options)
    options = {'path': dir1}
    resultArr = sm.storage.listPrefix(options)
    assert resultArr.__len__() == 1
    assert '/' + dir1 + '/a.txt' in resultArr
    assert '/' + dir1 + '/b.txt' not in resultArr


def test_task_output_put_get():
    newConfig = config.copy()
    newConfig.update({"clusterName": dir2})
    sm2 = StorageManager(newConfig)
    obj_path = sm2.hkube.put('myJobId', 'myTaksId', header=header, value=payload)
    assert obj_path == {'path': dir2 + '-hkube/myJobId/myTaksId'}
    (header1, payload1) = sm2.hkube.get('myJobId', 'myTaksId')
    decoded = encoding.decode(header=header1, value=payload1)
    assert decoded == raw
