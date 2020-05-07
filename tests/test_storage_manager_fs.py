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
encoded = encoding.encode({"data": 'all_my_data'})


@pytest.fixture(scope="session", autouse=True)
def beforeall(request):
    ensure_dir(baseDirectory)

    def afterall():
        shutil.rmtree(rootDirectory, ignore_errors=True)

    request.addfinalizer(afterall)


def test_put_get():
    options = {'path': dir1 + os.path.sep + 'a.txt', 'data': encoded}
    sm.storage.put(options)
    a = sm.storage.get(options)
    assert encoding.decode(a) == raw


def test_fail_to_get():
    options = {'path': dir1 + os.path.sep + 'no_such_path.txt', 'data': encoded}
    a = sm.storage.get(options)
    assert a == None


def test_list():
    options = {'path': dir1 + os.path.sep + 'a.txt', 'data': encoded}
    sm.storage.put(options)
    options = {'path': dir1 + os.path.sep + 'b.txt', 'data': encoded}
    sm.storage.put(options)
    options = {'path': dir1 + os.path.sep + 'inner' + os.path.sep + 'c.txt', 'data': encoded}
    sm.storage.put(options)
    options = {'path': dir1}
    resultArr = sm.storage.list(options)
    assert resultArr.__len__() == 3
    assert {'path': '/' + dir1 + '/a.txt'} in resultArr
    assert {'path': '/' + dir1 + '/b.txt'} in resultArr
    assert {'path': '/' + dir1 + '/inner/c.txt'} in resultArr


def test_prefixlist():
    options = {'path': dir1 + os.path.sep + 'a.txt', 'data': encoded}
    sm.storage.put(options)
    options = {'path': dir1 + os.path.sep + 'b.txt', 'data': encoded}
    sm.storage.put(options)
    options = {'path': dir1 + os.path.sep + 'inner' + os.path.sep + 'c.txt', 'data': encoded}
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
    options = {'path': dir1 + os.path.sep + 'a.txt', 'data': encoded}
    sm.storage.put(options)
    options = {'path': dir1 + os.path.sep + 'b.txt', 'data': encoded}
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
    obj_path = sm2.hkube.put('myJobId', 'myTaksId', encoded)
    assert obj_path == {'path': dir2 + '-hkube/myJobId/myTaksId'}
    a = sm2.hkube.get('myJobId', 'myTaksId')
    assert encoding.decode(a) == raw
