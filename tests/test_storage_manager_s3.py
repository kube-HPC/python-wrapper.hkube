import os
import pytest
from hkube_python_wrapper.util.encoding import Encoding
from hkube_python_wrapper.storage.storage_manager import StorageManager
from tests.configs import config

config = config.storage.copy()
config['type'] = 's3'

bucket = 'local-hkube'
encoding = Encoding(config['encoding'])

raw = {"data": 'all_my_data'}
encoded = encoding.encode({"data": 'all_my_data'})
sm = StorageManager(config)

@pytest.fixture(scope="session", autouse=True)
def beforeall():
    sm.storage.adapter.init({'bucket': bucket})


def test_put_get():
    options = {'path': bucket + os.path.sep + 'key1', 'data': encoded}
    sm.storage.put(options)
    a = sm.storage.get(options)
    assert encoding.decode(a) == raw


def test_fail_to_get():
    options = {'path': bucket + os.path.sep + 'no_such_key', 'data': encoded}

    with pytest.raises(Exception, match='The specified key does not exist'):
        sm.storage.get(options)


def test_list():
    bucketList = bucket + '-list'
    sm.storage.adapter.createBucket({'bucket': bucketList})
    options = {'path': bucketList + os.path.sep + 'list_key_1', 'data': encoded}
    sm.storage.put(options)
    options = {'path': bucketList + os.path.sep + 'list_key_2', 'data': encoded}
    sm.storage.put(options)
    options = {'path': bucketList + os.path.sep + 'innerList' + os.path.sep + 'list1', 'data': encoded}
    sm.storage.put(options)
    options = {'path': bucketList}
    resultArr = sm.storage.list(options)
    assert resultArr.__len__() == 3


def test_task_output_put_get():
    obj_path = sm.hkube.put('myJobId', 'myTaksId', encoded)
    assert obj_path == {'path': bucket + '/myJobId/myTaksId'}
    a = sm.hkube.get('myJobId', 'myTaksId')
    assert encoding.decode(a) == raw
