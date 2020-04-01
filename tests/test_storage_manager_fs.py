from storage.storage_manager import StorageManager
import pytest
import shutil
import os


def ensure_dir(f):
    d = os.path.dirname(f)
    if not os.path.exists(d):
        os.makedirs(d)
    return os.path.exists(f)


dir1 = 'dir1'
dir2 = 'dir2'

content = {"data": 'all_my_data'}
config = {
    "clusterName": os.environ.get('CLUSTER_NAME', 'local'),
    "type": os.environ.get('STORAGE_TYPE', 'fs'),
    "mode": os.environ.get('STORAGE_MODE', 'byRef'),
    "encoding": os.environ.get('STORAGE_ENCODING', 'bson'),
    "fs": {
        "baseDirectory": os.environ.get('BASE_FS_ADAPTER_DIRECTORY', 'baseDirectory')
    }
}

baseDirectory = config["fs"]['baseDirectory']


@pytest.fixture(scope="session", autouse=True)
def beforeall(request):
    ensure_dir('./' + baseDirectory)

    def afterall():
        shutil.rmtree(baseDirectory, ignore_errors=True)

    request.addfinalizer(afterall)


def test_put_get():
    sm = StorageManager(config)
    options = {'path': dir1 + os.path.sep + 'a.txt', 'data': content}
    sm.storage.put(options)
    a = sm.storage.get(options)
    assert a == content


def test_fail_to_get():
    options = {'path': dir1 + os.path.sep + 'no_such_path.txt', 'data': content}
    sm = StorageManager(config)
    a = sm.storage.get(options)
    assert a == None


def test_list():
    sm = StorageManager(config)
    options = {'path': dir1 + os.path.sep + 'a.txt', 'data': content}
    sm.storage.put(options)
    options = {'path': dir1 + os.path.sep + 'b.txt', 'data': content}
    sm.storage.put(options)
    options = {'path': dir1 + os.path.sep + 'inner' + os.path.sep + 'c.txt', 'data': content}
    sm.storage.put(options)
    options = {'path': dir1}
    resultArr = sm.storage.list(options)
    assert resultArr.__len__() == 3
    assert {'path': '/' + dir1 + '/a.txt'} in resultArr
    assert {'path': '/' + dir1 + '/b.txt'} in resultArr
    assert {'path': '/' + dir1 + '/inner/c.txt'} in resultArr


def test_prefixlist():
    sm = StorageManager(config)
    options = {'path': dir1 + os.path.sep + 'a.txt', 'data': content}
    sm.storage.put(options)
    options = {'path': dir1 + os.path.sep + 'b.txt', 'data': content}
    sm.storage.put(options)
    options = {'path': dir1 + os.path.sep + 'inner' + os.path.sep + 'c.txt', 'data': content}
    sm.storage.put(options)
    options = {'path': dir1}
    resultArr = sm.storage.listPrefix(options)
    assert resultArr.__len__() == 2
    assert '/' + dir1 + '/a.txt' in resultArr
    assert '/' + dir1 + '/b.txt' in resultArr
    assert '/' + dir1 + '/inner/c.txt' not in resultArr


def test_list_noneExsistingPath():
    sm = StorageManager(config)
    options = {'path': 'noneExisting'}
    result = sm.storage.list(options)
    assert result == None


def test_delete():
    sm = StorageManager(config)
    options = {'path': dir1 + os.path.sep + 'a.txt', 'data': content}
    sm.storage.put(options)
    options = {'path': dir1 + os.path.sep + 'b.txt', 'data': content}
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
    sm = StorageManager(newConfig)
    obj_path = sm.hkube.put('myJobId', 'myTaksId', content)
    assert obj_path == {'path': dir2 + '-hkube/myJobId/myTaksId'}
    a = sm.hkube.get('myJobId', 'myTaksId')
    assert a == content
