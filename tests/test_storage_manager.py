from storage.base_storage_manager import BaseStorageManager
from storage.task_output_manager import TaskOutputManager
from storage.fsAdapter import FSAdapter
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
config = {'baseDirectory': 'baseDirectory'}
ensure_dir('./' + config['baseDirectory'])


def test_put_get():
    sm = BaseStorageManager(FSAdapter(config, 'bson'))
    options = {'path': dirName + os.path.sep + 'a.txt', 'data': content}
    sm.put(options)
    a = sm.get(options)
    assert a == content


def test_fail_to_get():
    options = {'path': dirName + os.path.sep + 'a.txt', 'data': content}
    sm = BaseStorageManager(FSAdapter(config, 'bson'))
    a = sm.get(options)
    assert a == None


def test_list():
    sm = BaseStorageManager(FSAdapter(config, 'bson'))
    options = {'path': dirName + os.path.sep + 'a.txt', 'data': content}
    sm.put(options)
    options = {'path': dirName + os.path.sep + 'b.txt', 'data': content}
    sm.put(options)
    options = {'path': dirName + os.path.sep +
               'inner' + os.path.sep + 'c.txt', 'data': content}
    sm.put(options)
    options = {'path': dirName}
    resultArr = sm.list(options)
    assert resultArr.__len__() == 3
    assert '/myDir/a.txt' in resultArr
    assert '/myDir/b.txt' in resultArr
    assert '/myDir/inner/c.txt' in resultArr


def test_prefixlist():
    sm = BaseStorageManager(FSAdapter(config, 'bson'))
    options = {'path': dirName + os.path.sep + 'a.txt', 'data': content}
    sm.put(options)
    options = {'path': dirName + os.path.sep + 'b.txt', 'data': content}
    sm.put(options)
    options = {'path': dirName + os.path.sep +
               'inner' + os.path.sep + 'c.txt', 'data': content}
    sm.put(options)
    options = {'path': dirName}
    resultArr = sm.listPrefix(options)
    assert resultArr.__len__() == 2
    assert '/myDir/a.txt' in resultArr
    assert '/myDir/b.txt' in resultArr
    assert '/myDir/inner/c.txt' not in resultArr


def test_list_noneExsistingPath():
    sm = BaseStorageManager(FSAdapter(config, 'bson'))
    options = {'path': 'noneExisting'}
    result = sm.list(options)
    assert result == None


def test_delete():
    sm = BaseStorageManager(FSAdapter(config, 'bson'))
    options = {'path': dirName + os.path.sep + 'a.txt', 'data': content}
    sm.put(options)
    options = {'path': dirName + os.path.sep + 'b.txt', 'data': content}
    sm.put(options)
    sm.delete(options)
    options = {'path': dirName}
    resultArr = sm.listPrefix(options)
    assert resultArr.__len__() == 1
    assert '/myDir/a.txt' in resultArr
    assert '/myDir/b.txt' not in resultArr


def test_task_output_put_get():
    sm = TaskOutputManager(FSAdapter(config, 'bson'), {'clusterName': 'cName'})
    sm.put('myJobId', 'myTaksId', content)
    a = sm.get('myJobId', 'myTaksId')
    assert a == content


@pytest.fixture(scope="function", autouse=True)
def pytest_runtest_teardown(request):
    def remove_test_dir():
        shutil.rmtree(config['baseDirectory'] +
                      os.path.sep + dirName, ignore_errors=True)

    request.addfinalizer(remove_test_dir)
