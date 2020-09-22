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
    (header, payload) = sm.storage.get(options)
    assert encoding.decode(header=header, value=payload) == raw

def test_multi_parts():
    obj = {
        "bytesData": bytearray(b'\xdd'*(5)),
        "anotherBytesData": bytearray(5),
        "string": 'stam_data',
        "int": 42
    }
    (header, payload) = encoding.encode_separately(obj)
    options = {'path': bucket + os.path.sep + 'key2', 'header': header, 'data': payload}
    sm.storage.put(options)
    (header, payload) = sm.storage.get(options)
    decoded = encoding.decode(header=header, value=payload)
    assert decoded == obj
    

def test_fail_to_get():
    options = {'path': bucket + os.path.sep + 'no_such_key', 'data': encoded}

    with pytest.raises(Exception, match='Failed to read data from storage'):
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
    obj_path = sm.hkube.put('myJobId', 'myTaksId', value=encoded)
    assert obj_path == {'path': bucket + '/myJobId/myTaksId'}
    (header, payload) = sm.hkube.get('myJobId', 'myTaksId')
    assert encoding.decode(header=header, value=payload) == raw
