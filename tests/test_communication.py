import pytest
from gevent import monkey
import gevent
from tests.configs import config
from communication.DataRequest import DataRequest
from communication.DataServer import DataServer
from tests.mocks import mockdata
from util.encoding import Encoding

monkey.patch_all()
config = config.discovery

data1 = mockdata.dataTask1
data2 = mockdata.dataTask2
data2_original = mockdata.dataTask2_original
taskId1 = mockdata.taskId1
taskId2 = mockdata.taskId2
address1 = {'port': config['port'], 'host': config['host']}
address2 = {'port': "9021", 'host': config['host']}

encoding = Encoding(config['encoding'])
ds = None


def test_get_data_bytes():
    ds = DataServer(config)
    ds.setSendingState(mockdata.taskId2, encoding.encode(data2))
    ds.listen()
    dr = DataRequest({
        'address': address1,
        'taskId': taskId2,
        'dataPath': '',
        'encoding': config['encoding'],
        'timeout': '5'
    })
    reply = dr.invoke()
    assert reply == data2_original


def test_get_data_by_path():
    ds = DataServer(config)
    ds.setSendingState(mockdata.taskId1, encoding.encode(mockdata.dataTask1))
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
    ds.setSendingState(mockdata.taskId1, encoding.encode(mockdata.dataTask1))
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
    ds.setSendingState(mockdata.taskId1, encoding.encode(mockdata.dataTask1))
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
    ds.setSendingState(mockdata.taskId1, encoding.encode(mockdata.dataTask1))
    ds.listen()
    dr = DataRequest({
        'address': address1,
        'taskId': taskId1,
        'dataPath': '',
        'encoding': config['encoding']
    })
    reply = dr.invoke()
    assert reply == data1
    dr = DataRequest({
        'address': address1,
        'taskId': taskId1,
        'dataPath': '',
        'encoding': config['encoding']
    })
    reply = dr.invoke()
    assert reply == data1


def test_failing_to_get_data_old_task_id():
    ds = DataServer(config)
    ds.setSendingState(mockdata.taskId1, encoding.encode(mockdata.dataTask1))
    ds.listen()
    dr = DataRequest({
        'address':address1,
        'taskId': taskId1,
        'dataPath': '',
        'encoding': config['encoding'],
        'timeout':'5'
    })
    reply = dr.invoke()
    assert reply == mockdata.dataTask1
    ds.setSendingState(mockdata.taskId2, encoding.encode(mockdata.dataTask2))
    dr = DataRequest({
        'address':address1,
        'taskId': taskId1,
        'dataPath': '',
        'encoding': config['encoding'],
        'timeout': '5'
    })
    reply = dr.invoke()
    assert reply == {'hkube_error': {'message': 'Current taskId is task_2', 'code': 'notAvailable'}}


def test_failing_to_get_sending_ended():
    ds = DataServer(config)
    ds.setSendingState(mockdata.taskId1, encoding.encode(mockdata.dataTask1))
    ds.listen()
    dr = DataRequest({
        'address':address1,
        'taskId': taskId1,
        'dataPath': '',
        'encoding': config['encoding'],
        'timeout':'5'
    })
    reply = dr.invoke()
    assert reply == mockdata.dataTask1
    ds.endSendingState()
    dr = DataRequest({
        'address':address1,
        'taskId': taskId1,
        'dataPath': '',
        'encoding': config['encoding']
        ,'timeout':'5'
    })
    reply = dr.invoke()
    assert reply == {'hkube_error': {'message': 'Current taskId is None', 'code': 'notAvailable'}}


def test_isServing():
    ds = DataServer(config)
    ds.setSendingState(mockdata.taskId1, encoding.encode(mockdata.dataTask1))
    ds.listen()
    def sleepNow(message):
        gevent.sleep(3)
        return ds.createReply(message)
    ds._adapter.getReplyFunc = sleepNow
    ds.setSendingState(mockdata.dataTask1, data1)
    dr = DataRequest(
        {'address':address1, 'taskId': taskId1, 'dataPath': 'level1',
         'encoding': 'bson','timeout':'5'})
    gevent.spawn(dr.invoke)
    gevent.sleep(1)
    assert ds.isServing() == True
    gevent.sleep(3)
    assert ds.isServing() == False


def test_waitTillServingEnds():
    ds = DataServer(config)
    ds.setSendingState(mockdata.taskId1, encoding.encode(mockdata.dataTask1))
    ds.listen()
    def sleepNow(message):
        gevent.sleep(3)
        return ds.createReply(message)
    ds._adapter.getReplyFunc = sleepNow
    ds.setSendingState(mockdata.dataTask1, data1)
    dr = DataRequest(
        {'address':address1, 'taskId': taskId1, 'dataPath': 'level1',
         'encoding': 'bson','timeout':'5'})
    gevent.spawn(dr.invoke)
    gevent.sleep(1)
    assert ds.isServing() == True
    ds.waitTillServingEnds()
    assert ds.isServing() == False

@pytest.fixture(scope="function", autouse=True)
def pytest_runtest_teardown(request):
    def closeResource():
        if (ds != None):
            ds.close()

    request.addfinalizer(closeResource)
