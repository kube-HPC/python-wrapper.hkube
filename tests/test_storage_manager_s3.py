import pytest
import os
from storage.storage_manager import StorageManager
import tests.configs.config as conf
config = conf.Config
config = config.storage
config['type'] = 's3'
content = {"data": 'all_my_data'}

bucket = 'local-hkube'


@pytest.fixture(scope="session", autouse=True)
def beforeall(request):
    sm = StorageManager(config)
    sm.storage.adapter.init({'bucket': bucket})


def test_put_get():
    sm = StorageManager(config)
    options = {'path': bucket + os.path.sep + 'key1', 'data': content}
    sm.storage.put(options)
    a = sm.storage.get(options)
    assert a == content


def test_fail_to_get():
    options = {'path': bucket + os.path.sep + 'no_such_key', 'data': content}
    sm = StorageManager(config)

    with pytest.raises(Exception, match='The specified key does not exist'):
        a = sm.storage.get(options)


def test_list():
    bucketList = bucket + '-list'
    sm = StorageManager(config)
    sm.storage.adapter.createBucket({'bucket': bucketList})
    options = {'path': bucketList + os.path.sep + 'list_key_1', 'data': content}
    sm.storage.put(options)
    options = {'path': bucketList + os.path.sep + 'list_key_2', 'data': content}
    sm.storage.put(options)
    options = {'path': bucketList + os.path.sep + 'innerList' + os.path.sep + 'list1', 'data': content}
    sm.storage.put(options)
    options = {'path': bucketList}
    resultArr = sm.storage.list(options)
    assert resultArr.__len__() == 3


def test_task_output_put_get():
    sm = StorageManager(config)
    obj_path = sm.hkube.put('myJobId', 'myTaksId', content)
    assert obj_path == {'path': bucket + '/myJobId/myTaksId'}
    a = sm.hkube.get('myJobId', 'myTaksId')
    assert a == content
