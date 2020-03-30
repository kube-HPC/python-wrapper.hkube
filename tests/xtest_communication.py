
import tests.configs.config as conf
from communication.DataRequest import DataRequest
import pytest
import gevent
from gevent import monkey
monkey.patch_all()
config = conf.Config
config = config.discovery
reources = {}

task1 = 'task_1'
data2 = {
    'level1': {
        'level2': {
            'value1': 'd2_l1_l2_value_1',
            'value2': 'd2_l1_l2_value_2',
        },
        'value1': 'd2_l1_value_1'
    },
    'value1': 'd2_value_1'
}
data3 = bytearray(1024 * 1024 * 100)


def test_get_data_by_path():

    dr = DataRequest(
        {'address': {'port': config['port'], 'host': config['host']}, 'taskId': task1, 'dataPath': 'level1',
         'encoding': config['encoding']})
    reply = dr.invoke()
    assert reply['data'] == {
        'level2': {
            'value1': 'l1_l2_value_1',
            'value2': 'l1_l2_value_2',
        },
        'value1': 'l1_value_1'
    }
    dr = DataRequest(
        {'address': {'port': config['port'], 'host': config['host']}, 'taskId': task1, 'dataPath': 'value1',
         'encoding': 'bson'})
    reply = dr.invoke()
    assert reply['data'] == 'value_1'


def test_path_not_exist():

    dr = DataRequest(
        {'address': {'port': config['port'], 'host': config['host']}, 'taskId': task1, 'dataPath': 'notExist',
         'encoding': 'bson'})
    reply = dr.invoke()
    assert reply == {u'error': {u'message': "'notExist'", u'code': u'unknown'}}


def test_get_complete_data():

    dr = DataRequest(
        {'address': {'port': config['port'], 'host': config['host']}, 'taskId': task1, 'dataPath': '',
         'encoding': 'bson'})
    reply = dr.invoke()
    assert reply['data'] == data1
    dr = DataRequest(
        {'address': {'port': config['port'], 'host': config['host']}, 'taskId': task1, 'dataPath': 'value1',
         'encoding': 'bson'})
    reply = dr.invoke()
    assert reply['data'] == 'value_1'


def test_data_after_taskid_changed():

    dr = DataRequest(
        {'address': {'port': config['port'], 'host': config['host']}, 'taskId': task1, 'dataPath': '',
         'encoding': 'bson'})
    reply = dr.invoke()
    assert reply['data'] == data1
    # reources['ds'].setSendingState(task2, data2)
    dr = DataRequest(
        {'address': {'port': config['port'], 'host': config['host']}, 'taskId': task2, 'dataPath': '',
         'encoding': 'bson'})
    reply = dr.invoke()
    assert reply['data'] == data2


def test_failing_to_get_data_old_task_id():

    dr = DataRequest(
        {'address': {'port': config['port'], 'host': config['host']}, 'taskId': task1, 'dataPath': '',
         'encoding': 'bson'})
    reply = dr.invoke()
    assert reply['data'] == data1
    # reources['ds'].setSendingState(task2, data2)
    dr = DataRequest(
        {'address': {'port': config['port'], 'host': config['host']}, 'taskId': task1, 'dataPath': '',
         'encoding': 'bson'})
    reply = dr.invoke()
    assert reply == {u'error': {u'message': u'Current taskId is task_2', u'code': u'notAvailable'}}


def test_failing_to_get_sending_ended():

    dr = DataRequest(
        {'address': {'port': config['port'], 'host': config['host']}, 'taskId': task1, 'dataPath': '',
         'encoding': 'bson'})
    reply = dr.invoke()
    assert reply['data'] == data1
    # reources['ds'].endSendingState()
    dr = DataRequest(
        {'address': {'port': config['port'], 'host': config['host']}, 'taskId': task1, 'dataPath': '',
         'encoding': 'bson'})
    reply = dr.invoke()
    assert reply == {u'error': {u'message': u'Current taskId is None', u'code': u'notAvailable'}}


# def test_isServing():

#     def sleepNow(message):
#         gevent.sleep(3)
#         return reources['ds'].createReply(message)
#     reources['ds'].adpater.getReplyFunc = sleepNow
#     reources['ds'].setSendingState(task1, data1)

#     dr = DataRequest(
#         {'address': {'port': config['port'], 'host': config['host']}, 'taskId': task1, 'dataPath': 'level1',
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
