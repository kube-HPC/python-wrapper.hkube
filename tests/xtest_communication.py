import threading
import pytest
import gevent
from hkube_python_wrapper.communication.DataRequest import DataRequest
from hkube_python_wrapper.communication.DataServer import DataServer
from hkube_python_wrapper.util.encoding import Encoding
from tests.mocks import mockdata
from tests.configs import config

discovery = dict(config.discovery)
discovery.update({"port": 9024})
data1 = mockdata.dataTask1
data2 = mockdata.dataTask2
data2_original = mockdata.dataTask2_original
taskId1 = mockdata.taskId1
taskId2 = mockdata.taskId2
address1 = {'port': "9024", 'host': discovery['host']}

encoding = Encoding(discovery['encoding'])
timeout = 5000

ds = DataServer(discovery)
ds.setSendingState(mockdata.taskId1, data1)
ds.setSendingState(mockdata.taskId2, data2)
ds.setSendingState(mockdata.taskId1, mockdata.dataTask1)
ds.setSendingState(mockdata.taskId2, mockdata.dataTask2)

thread = threading.Thread(target=ds.listen)
thread.start()
gevent.sleep(2)

def test_get_data_bytes():

    dr = DataRequest({
        'address': address1,
        'taskId': mockdata.taskId2,
        'encoding': discovery['encoding'],
        'timeout': timeout
    })
    reply = dr.invoke()
    assert reply == data2_original


def test_get_data_by_path():
    dr = DataRequest({
        'address': address1,
        'taskId': taskId1,
        'dataPath': 'level1',
        'encoding': discovery['encoding'],
        'timeout': timeout
    })
    reply = dr.invoke()
    assert reply == data1['level1']
    dr = DataRequest({
        'address': address1,
        'taskId': taskId1,
        'dataPath': 'value1',
        'encoding': discovery['encoding'],
        'timeout': timeout
    })
    reply = dr.invoke()
    assert reply == data1['value1']


def test_path_not_exist():
    dr = DataRequest({
            'address': address1,
            'taskId': taskId1,
            'dataPath': 'notExist',
            'encoding': discovery['encoding'],
            'timeout': timeout
        })
    reply = dr.invoke()
    assert reply == {'hkube_error': {'code': 'noSuchDataPath', 'message': "notExist does not exist in data"}}


def test_get_complete_data():
    dr = DataRequest({
        'address': address1,
        'taskId': taskId1,
        'dataPath': '',
        'encoding': discovery['encoding'],
        'timeout': timeout
    })
    reply = dr.invoke()
    assert reply == data1


def test_data_after_taskid_changed():
    dr = DataRequest({
        'address': address1,
        'taskId': taskId1,
        'dataPath': '',
        'encoding': discovery['encoding'],
        'timeout': timeout
    })
    reply = dr.invoke()
    assert reply == data1
    dr = DataRequest({
        'address': address1,
        'taskId': taskId1,
        'dataPath': '',
        'encoding': discovery['encoding'],
        'timeout': timeout
    })
    reply = dr.invoke()
    assert reply == data1


def test_success_to_get_data_old_task_id():
    dr = DataRequest({
        'address': address1,
        'taskId': taskId1,
        'dataPath': '',
        'encoding': discovery['encoding'],
        'timeout': timeout
    })
    reply = dr.invoke()
    assert reply == mockdata.dataTask1
    dr = DataRequest({
        'address': address1,
        'taskId': taskId2,
        'dataPath': '',
        'encoding': discovery['encoding'],
        'timeout': timeout
    })
    reply = dr.invoke()
    assert reply == mockdata.dataTask2_original


def test_failing_no_such_taskid():
    taskId = 'no_such_taskid'
    dr = DataRequest({
        'address': address1,
        'taskId': taskId,
        'dataPath': '',
        'encoding': discovery['encoding'],
        'timeout': timeout
    })
    reply = dr.invoke()
    assert reply == {'hkube_error': {'code': 'notAvailable', 'message': 'taskId notAvailable'}}


def test_isServing():
    ds = DataServer(discovery)
    gevent.spawn(ds.listen)
    gevent.sleep(0.5)
    assert ds.isServing() == False


def test_shutDown():
    ds = DataServer(discovery)
    gevent.spawn(ds.listen)
    gevent.sleep(0.5)
    ds.shutDown()
    assert ds.isServing() == False


def xtest_waitTillServingEnds():
    def sleepNow(message):
        gevent.sleep(3)
        return ds._createReply(message)
    ds._adapter.getReplyFunc = sleepNow
    dr = DataRequest(
        {'address': address1, 'taskId': taskId1, 'dataPath': 'level1',
         'encoding': 'bson', 'timeout': timeout})
    gevent.spawn(dr.invoke)
    gevent.sleep(1)
    assert ds.isServing() == True
    ds.shutDown()
    assert ds.isServing() == False


def test_fail_on_timeout():
    dr = DataRequest({
        'address': address1,
        'taskId': taskId1,
        'dataPath': '',
        'encoding': discovery['encoding'],
        'timeout': timeout
    })
    reply = dr.invoke()
    assert reply == {'hkube_error': {'code': 'unknown', 'message': 'Timed out:5000'}}
