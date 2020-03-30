from storage.storage_manager import StorageManager
import pytest
import shutil
import os


def ensure_dir(f):
    d = os.path.dirname(f)
    if not os.path.exists(d):
        os.makedirs(d)
    return os.path.exists(f)


content = {"data": 'all_my_data'}
dirName = 'myDir'
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


@pytest.fixture(scope="function", autouse=True)
def pytest_runtest_teardown(request):
    def remove_test_dir():
        shutil.rmtree(baseDirectory + os.path.sep + dirName, ignore_errors=True)

    request.addfinalizer(remove_test_dir)


def test_put_get():
    sm = StorageManager(config)
    options = {'path': dirName + os.path.sep + 'a.txt', 'data': content}
    sm.storage.put(options)
    a = sm.storage.get(options)
    assert a == content


def test_fail_to_get():
    options = {'path': dirName + os.path.sep + 'a.txt', 'data': content}
    sm = StorageManager(config)
    a = sm.storage.get(options)
    assert a == None


def test_list():
    sm = StorageManager(config)
    options = {'path': dirName + os.path.sep + 'a.txt', 'data': content}
    sm.storage.put(options)
    options = {'path': dirName + os.path.sep + 'b.txt', 'data': content}
    sm.storage.put(options)
    options = {'path': dirName + os.path.sep + 'inner' + os.path.sep + 'c.txt', 'data': content}
    sm.storage.put(options)
    options = {'path': dirName}
    resultArr = sm.storage.list(options)
    assert resultArr.__len__() == 3
    assert {'path': '/myDir/a.txt'} in resultArr
    assert {'path': '/myDir/b.txt'} in resultArr
    assert {'path': '/myDir/inner/c.txt'} in resultArr


def test_prefixlist():
    sm = StorageManager(config)
    options = {'path': dirName + os.path.sep + 'a.txt', 'data': content}
    sm.storage.put(options)
    options = {'path': dirName + os.path.sep + 'b.txt', 'data': content}
    sm.storage.put(options)
    options = {'path': dirName + os.path.sep + 'inner' + os.path.sep + 'c.txt', 'data': content}
    sm.storage.put(options)
    options = {'path': dirName}
    resultArr = sm.storage.listPrefix(options)
    assert resultArr.__len__() == 2
    assert '/myDir/a.txt' in resultArr
    assert '/myDir/b.txt' in resultArr
    assert '/myDir/inner/c.txt' not in resultArr


def test_list_noneExsistingPath():
    sm = StorageManager(config)
    options = {'path': 'noneExisting'}
    result = sm.storage.list(options)
    assert result == None


def test_delete():
    sm = StorageManager(config)
    options = {'path': dirName + os.path.sep + 'a.txt', 'data': content}
    sm.storage.put(options)
    options = {'path': dirName + os.path.sep + 'b.txt', 'data': content}
    sm.storage.put(options)
    sm.storage.delete(options)
    options = {'path': dirName}
    resultArr = sm.storage.listPrefix(options)
    assert resultArr.__len__() == 1
    assert '/myDir/a.txt' in resultArr
    assert '/myDir/b.txt' not in resultArr


def test_task_output_put_get():
    newConfig = config
    newConfig.update({"clusterName": "cName"})
    sm = StorageManager(config)
    obj_path = sm.hkube.put('myJobId', 'myTaksId', content)
    assert obj_path == {'path': 'cName-hkube/myJobId/myTaksId'}
    a = sm.hkube.get('myJobId', 'myTaksId')
    assert a == content
