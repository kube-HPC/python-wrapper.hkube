
import tests.configs.config as conf
from communication.DataRequest import DataRequest
import pytest
import gevent
from gevent import monkey

monkey.patch_all()
config = conf.Config
config = config.discovery
reources = {}

data3 = bytearray(1024 * 1024 * 100)
taskId = 'task_1'
data = {
    'level1': {
        'level2': {
            'value1': 'd2_l1_l2_value_1',
            'value2': 'd2_l1_l2_value_2',
        },
        'value1': 'd2_l1_value_1'
    },
    'value1': 'd2_value_1'
}


def test_get_data_by_path():
    dr = DataRequest({
        'address': {'port': config['port'], 'host': config['host']},
        'taskId': taskId,
        'dataPath': 'level1',
        'encoding': config['encoding']
    })
    reply = dr.invoke()
    assert reply['data'] == data['level1']
    dr = DataRequest({
        'address': {'port': config['port'], 'host': config['host']},
        'taskId': taskId,
        'dataPath': 'value1',
        'encoding': config['encoding']
    })
    reply = dr.invoke()
    assert reply['data'] == data['value1']


def test_path_not_exist():
    dr = DataRequest(
        {
            'address': {'port': config['port'], 'host': config['host']},
            'taskId': taskId,
            'dataPath': 'notExist',
            'encoding': config['encoding']
        })
    reply = dr.invoke()
    assert reply == {'error': {'code': 'noSuchDataPath', 'message': "notExist does not exist in data"}}


def test_get_complete_data():

    dr = DataRequest({
        'address': {'port': config['port'], 'host': config['host']},
        'taskId': taskId,
        'dataPath': '',
        'encoding': config['encoding']
    })
    reply = dr.invoke()
    assert reply['data'] == data
    dr = DataRequest({
        'address': {'port': config['port'], 'host': config['host']},
        'taskId': taskId,
        'dataPath': 'value1',
        'encoding': config['encoding']
    })
    reply = dr.invoke()
    assert reply['data'] == data['value1']


def test_data_after_taskid_changed():

    dr = DataRequest({
        'address': {'port': config['port'], 'host': config['host']},
        'taskId': taskId,
        'dataPath': '',
        'encoding': config['encoding']
    })
    reply = dr.invoke()
    assert reply['data'] == data
    dr = DataRequest({
        'address': {'port': config['port'], 'host': config['host']},
        'taskId': taskId,
        'dataPath': '',
        'encoding': config['encoding']
    })
    reply = dr.invoke()
    assert reply['data'] == data


# def test_failing_to_get_data_old_task_id():

#     dr = DataRequest({
#         'address': {'port': config['port'], 'host': config['host']},
#         'taskId': taskId,
#         'dataPath': '',
#         'encoding': config['encoding']
#     })
#     reply = dr.invoke()
#     assert reply['data'] == data
#     dr = DataRequest({
#         'address': {'port': config['port'], 'host': config['host']},
#         'taskId': taskId,
#         'dataPath': '',
#         'encoding': config['encoding']
#     })
#     reply = dr.invoke()
#     assert reply == {'error': {'message': 'Current taskId is task_2', 'code': 'notAvailable'}}


# def test_failing_to_get_sending_ended():

#     dr = DataRequest({
#         'address': {'port': config['port'], 'host': config['host']},
#         'taskId': taskId,
#         'dataPath': '',
#         'encoding': config['encoding']
#     })
#     reply = dr.invoke()
#     assert reply['data'] == data
#     dr = DataRequest({
#         'address': {'port': config['port'], 'host': config['host']},
#         'taskId': taskId,
#         'dataPath': '',
#         'encoding': config['encoding']
#     })
#     reply = dr.invoke()
#     assert reply == {'error': {'message': 'Current taskId is None', 'code': 'notAvailable'}}


# def test_isServing():

#     def sleepNow(message):
#         gevent.sleep(3)
#         return reources['ds'].createReply(message)
#     reources['ds'].adpater.getReplyFunc = sleepNow
#     reources['ds'].setSendingState(taskId, data1)

#     dr = DataRequest(
#         {'address': {'port': config['port'], 'host': config['host']}, 'taskId': taskId, 'dataPath': 'level1',
#          'encoding': 'bson'})
#     gevent.spawn(dr.invoke)
#     gevent.sleep(1)
#     assert reources['ds'].isServing() == True
#     gevent.sleep(3)
#     assert reources['ds'].isServing() == False


# @pytest.fixture(scope="function", autouse=True)
# def pytest_runtest_teardown(request):
#     def closeResource():
#         reources['ds'].close()

#     request.addfinalizer(closeResource)
