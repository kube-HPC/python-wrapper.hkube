from storage_manager.storage_manager import StorageManager
import os
import pytest

content = 'all_my_data'
# if __name__ == '__main__':
def test_put_get():
    print('\ntest_put_get!!!!')
    config = {'baseDirectory': '~/'}
    sm = StorageManager(config)
    options  = {}
    options['fileName'] = 'a.txt'
    options['directoryName'] = 'myDir'
    options['data'] = content
    sm.put(options)
    a = sm.get(options)
    assert a == content
def test_list_dir():
    pass

@pytest.fixture(scope="function", autouse=True)
def pytest_runtest_teardown(request):
    def remove_test_dir():
        os.rmdir('~/myDir')
    request.addfinalizer(remove_test_dir)