import os
import pytest
from hkube_python_wrapper.util.encoding import Encoding
from hkube_python_wrapper.storage.storage_manager import StorageManager
from tests.configs import config as config_orig

config = config_orig.storage.copy()
config['type'] = 's3'

bucket = 'local-hkube'
encoding = Encoding(config['encoding'])

raw = {"data": 'all_my_data'}
(header, payload) = encoding.encode(raw)
sm = StorageManager(config)

@pytest.fixture(scope="session", autouse=True)
def beforeall():
    sm.storage.adapter.init({'bucket': bucket})


def test_put_get():
    options = {'path': bucket + os.path.sep + 'key1', 'header': header, 'data': payload}
    sm.storage.put(options)
    (header1, payload1) = sm.storage.get(options)
    assert encoding.decode(header=header1, value=payload1) == raw

def test_multi_parts():
    obj = {
        "bytesData": bytearray(b'\xdd'*(5)),
        "anotherBytesData": bytearray(5),
        "string": 'stam_data',
        "int": 42
    }
    (header1, payload1) = encoding.encode(obj)
    options = {'path': bucket + os.path.sep + 'key2', 'header': header1, 'data': payload1}
    sm.storage.put(options)
    (header2, payload2) = sm.storage.get(options)
    decoded = encoding.decode(header=header2, value=payload2)
    assert decoded == obj

def test_fail_to_get():
    options = {'path': bucket + os.path.sep + 'no_such_key', 'header': header, 'data': payload}

    with pytest.raises(Exception, match='Failed to read data from storage'):
        sm.storage.get(options)


def test_list():
    bucketList = bucket + '-list'
    sm.storage.adapter.createBucket({'bucket': bucketList})
    options = {'path': bucketList + os.path.sep + 'list_key_1', 'header': header, 'data': payload}
    sm.storage.put(options)
    options = {'path': bucketList + os.path.sep + 'list_key_2', 'header': header, 'data': payload}
    sm.storage.put(options)
    options = {'path': bucketList + os.path.sep + 'innerList' + os.path.sep + 'list1', 'header': header, 'data': payload}
    sm.storage.put(options)
    options = {'path': bucketList}
    resultArr = sm.storage.list(options)
    assert resultArr.__len__() == 3


def test_task_output_put_get():
    obj_path = sm.hkube.put('myJobId', 'myTaksId', header=header, value=payload)
    assert obj_path == {'path': bucket + '/myJobId/myTaksId'}
    (header1, payload1) = sm.hkube.get('myJobId', 'myTaksId')
    assert encoding.decode(header=header1, value=payload1) == raw

def test_s3_ssl():
    config_ss3 = config_orig.storage.copy()
    config_ss3['type'] = 's3'
    config_ss3['s3']['verify_ssl'] = False
    config_ss3['s3']['endpoint']='https://127.0.0.1:9001'
    
    sm_ssl = StorageManager(config_ss3)
    sm_ssl.storage.adapter.init({'bucket': bucket})
    options = {'path': bucket + os.path.sep + 'key1', 'header': header, 'data': payload}
    sm_ssl.storage.put(options)
    (header1, payload1) = sm_ssl.storage.get(options)
    assert encoding.decode(header=header1, value=payload1) == raw