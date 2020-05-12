import pytest
import gevent
from hkube_python_wrapper.communication.DataRequest import DataRequest
from hkube_python_wrapper.communication.DataServer import DataServer
from hkube_python_wrapper.util.encoding import Encoding
from tests.mocks import mockdata
from tests.configs import config

config = config.discovery

data1 = mockdata.dataTask1
data2 = mockdata.dataTask2
data2_original = mockdata.dataTask2_original
taskId1 = mockdata.taskId1
taskId2 = mockdata.taskId2
address1 = {'port': config['port'], 'host': config['host']}
address2 = {'port': "9021", 'host': config['host']}

encoding = Encoding(config['encoding'])
resources = {}
resources['ds'] = None


def test_get_data_bytes():
    ds = DataServer(config)
    resources['ds'] = ds
    ds.setSendingState(mockdata.taskId2, data2)
    ds.listen()
    dr = DataRequest({
        'address': address1,
        'taskId': mockdata.taskId2,
        'encoding': config['encoding'],
        'timeout': '5'
    })
    reply = dr.invoke()
    assert reply == data2_original


def test_get_data_by_path():
    ds = DataServer(config)
    resources['ds'] = ds
    ds.setSendingState(mockdata.taskId1, mockdata.dataTask1)
    ds.listen()
    gevent.sleep()
    dr = DataRequest({
        'address': address1,
        'taskId': taskId1,
        'dataPath': 'level1',
        'encoding': config['encoding'],
        'timeout': '5'
    })
    reply = dr.invoke()
    assert reply == data1['level1']
    dr = DataRequest({
        'address': address1,
        'taskId': taskId1,
        'dataPath': 'value1',
        'encoding': config['encoding'],
        'timeout': '5'
    })
    reply = dr.invoke()
    assert reply == data1['value1']


def test_path_not_exist():
    ds = DataServer(config)
    resources['ds'] = ds
    ds.setSendingState(mockdata.taskId1, mockdata.dataTask1)
    ds.listen()
    dr = DataRequest(
        {
            'address': address1,
            'taskId': taskId1,
            'dataPath': 'notExist',
            'encoding': config['encoding'],
            'timeout': '5'
        })
    reply = dr.invoke()
    assert reply == {'hkube_error': {'code': 'noSuchDataPath', 'message': "notExist does not exist in data"}}


def test_get_complete_data():
    ds = DataServer(config)
    resources['ds'] = ds
    ds.setSendingState(mockdata.taskId1, mockdata.dataTask1)
    ds.listen()
    dr = DataRequest({
        'address': address1,
        'taskId': taskId1,
        'dataPath': '',
        'encoding': config['encoding'],
        'timeout': '5'
    })
    reply = dr.invoke()
    assert reply == data1


def test_data_after_taskid_changed():
    ds = DataServer(config)
    resources['ds'] = ds
    ds.setSendingState(mockdata.taskId1, mockdata.dataTask1)
    ds.listen()
    dr = DataRequest({
        'address': address1,
        'taskId': taskId1,
        'dataPath': '',
        'encoding': config['encoding'],
        'timeout': '5'
    })
    reply = dr.invoke()
    assert reply == data1
    dr = DataRequest({
        'address': address1,
        'taskId': taskId1,
        'dataPath': '',
        'encoding': config['encoding'],
        'timeout': '5'
    })
    reply = dr.invoke()
    assert reply == data1


def test_success_to_get_data_old_task_id():
    ds = DataServer(config)
    resources['ds'] = ds
    ds.setSendingState(mockdata.taskId1, mockdata.dataTask1)
    ds.listen()
    dr = DataRequest({
        'address': address1,
        'taskId': taskId1,
        'dataPath': '',
        'encoding': config['encoding'],
        'timeout': '5'
    })
    reply = dr.invoke()
    assert reply == mockdata.dataTask1
    ds.setSendingState(mockdata.taskId2, mockdata.dataTask2)
    dr = DataRequest({
        'address': address1,
        'taskId': taskId2,
        'dataPath': '',
        'encoding': config['encoding'],
        'timeout': '5'
    })
    reply = dr.invoke()
    assert reply == mockdata.dataTask2_original


def test_failing_no_such_taskid():
    ds = DataServer(config)
    resources['ds'] = ds
    ds.setSendingState(mockdata.taskId1, mockdata.dataTask1)
    ds.listen()
    taskId = 'no_such_taskid'
    dr = DataRequest({
        'address': address1,
        'taskId': taskId,
        'dataPath': '',
        'encoding': config['encoding'],
        'timeout': '5'
    })
    reply = dr.invoke()
    assert reply == {'hkube_error': {'code': 'notAvailable', 'message': 'taskId ' + taskId + ' notAvailable'}}


def test_isServing():
    ds = DataServer(config)
    resources['ds'] = ds
    ds.setSendingState(mockdata.taskId1, mockdata.dataTask1)
    ds.listen()

    def sleepNow(message):
        gevent.sleep(3)
        return ds.createReply(message)
    ds._adapter.getReplyFunc = sleepNow
    ds.setSendingState(mockdata.taskId1, data1)
    dr = DataRequest(
        {'address': address1, 'taskId': taskId1, 'dataPath': 'level1',
         'encoding': 'bson', 'timeout': '5'})
    gevent.spawn(dr.invoke)
    gevent.sleep(1)
    assert ds.isServing() == True
    gevent.sleep(3)
    assert ds.isServing() == False


def test_waitTillServingEnds():
    ds = DataServer(config)
    resources['ds'] = ds
    ds.setSendingState(mockdata.taskId1, mockdata.dataTask1)
    ds.listen()

    def sleepNow(message):
        gevent.sleep(3)
        return ds.createReply(message)
    ds._adapter.getReplyFunc = sleepNow
    ds.setSendingState(mockdata.taskId1, data1)
    dr = DataRequest(
        {'address': address1, 'taskId': taskId1, 'dataPath': 'level1',
         'encoding': 'bson', 'timeout': '5'})
    gevent.spawn(dr.invoke)
    gevent.sleep(1)
    assert ds.isServing() == True
    ds.waitTillServingEnds()
    assert ds.isServing() == False


def test_fail_on_timeout():
    dr = DataRequest({
        'address': address1,
        'taskId': taskId1,
        'dataPath': '',
        'encoding': config['encoding'],
        'timeout': '5'
    })
    reply = dr.invoke()
    assert reply == {'hkube_error': {'code': 'unknown', 'message': 'Timed out:5000'}}


@pytest.fixture(scope="function", autouse=True)
def pytest_runtest_teardown(request):
    def closeResource():
        if (resources['ds'] != None):
            resources['ds'].close()
            resources['ds'] = None
    request.addfinalizer(closeResource)
