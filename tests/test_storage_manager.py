from storage.storage_manager import StorageManager
import pytest
import shutil
import os


def ensure_dir(f):
    d = os.path.dirname(f)
    if not os.path.exists(d):
        os.makedirs(d)
    return os.path.exists(f)


content = 'all_my_data'
dirName = 'myDir'
config = {'baseDirectory': 'baseDirectory'}
ensure_dir('./' + config['baseDirectory'])


# if __name__ == '__main__':
def test_put_get():
    print('\ntest_put_get!!!!')
    sm = StorageManager(config)
    options = {'fileName': 'a.txt', 'directoryName': dirName, 'data': content}
    sm.put(options)
    a = sm.get(options)
    assert a == content


def test_fail_to_get():
    options = {'fileName': 'a.txt', 'directoryName': dirName, 'data': content}
    sm = StorageManager(config)
    a = sm.get(options)
    assert a == None


def test_list():
    sm = StorageManager(config)
    options = {'fileName': 'a.txt', 'directoryName': dirName, 'data': content}
    sm.put(options)
    options = {'fileName': 'b.txt', 'directoryName': dirName, 'data': content}
    sm.put(options)
    options = {'fileName': 'c.txt', 'directoryName': dirName + os.path.sep + 'inner', 'data': content}
    sm.put(options)
    options = {'path': dirName}
    resultArr = sm.list(options)
    assert resultArr.__len__() == 3
    assert '/myDir/a.txt' in resultArr
    assert '/myDir/b.txt' in resultArr
    assert '/myDir/inner/c.txt' in resultArr


def test_list_nonExsisting():
    sm = StorageManager(config)
    options = {'path': 'noneExisting'}
    result = sm.list(options)
    assert result == None


def test_get_putStream():
    sm = StorageManager(config)
    options = {'fileName': 'a.txt', 'directoryName': dirName, 'data': content}
    sm.put(options)
    stream = sm.getStream(options)
    options = {'fileName': 'aa.txt', 'directoryName': dirName, 'data': stream}
    sm.putStream(options)
    options = {'fileName': 'aa.txt', 'directoryName': dirName}
    aa = sm.get(options)
    assert aa == content

@pytest.fixture(scope="function", autouse=True)
def pytest_runtest_teardown(request):
    def remove_test_dir():
        shutil.rmtree(config['baseDirectory'] + os.path.sep + dirName, ignore_errors=True)

    request.addfinalizer(remove_test_dir)
