import zmq

from communication.DataRequest import DataRequest
from communication.DataServer import DataServer

import gevent

config = {
    'port': 3003,
    'host': 'localhost',
    'encoding': 'bson'
}
task1 = 'task_1'
task2 = 'task_2'
data1 = {
    'level1': {
        'level2': {
            'value1': 'l1_l2_value_1',
            'value2': 'l1_l2_value_2',
        },
        'value1': 'l1_value_1'
    },
    'value1': 'value_1'
};
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
 
        ds = DataServer({'port': config['port'], 'encoding': 'bson'})
        ds.setSendingState(task1, data1)
        gevent.sleep(1)
        dr = DataRequest(
            {'address': {'port': config['port'], 'host': config['host']}, 'taskId': task1, 'dataPath': 'level1',
             'encoding': 'bson'})
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
    ds = DataServer({'port': config['port'], 'encoding': 'bson'})
    ds.setSendingState(task1, data1)
    gevent.sleep(1)
    dr = DataRequest(
        {'address': {'port': config['port'], 'host': config['host']}, 'taskId': task1, 'dataPath': 'notExist',
         'encoding': 'bson'})
    reply = dr.invoke()
    assert reply == {u'error': {u'message': u"u'notExist'", u'code': u'unknown'}}

def test_get_complete_data():
    ds = DataServer({'port': config['port'], 'encoding': 'bson'})
    ds.setSendingState(task1, data1)
    gevent.sleep(1)
    dr = DataRequest(
        {'address': {'port': config['port'], 'host': config['host']}, 'taskId': task1, 'dataPath': '',
         'encoding': 'bson'})
    reply = dr.invoke()
    assert reply ['data']== data1
    dr = DataRequest(
        {'address': {'port': config['port'], 'host': config['host']}, 'taskId': task1, 'dataPath': 'value1',
         'encoding': 'bson'})
    reply = dr.invoke()
    assert reply['data'] == 'value_1'

def test_data_after_taskid_changed():
    ds = DataServer({'port': config['port'], 'encoding': 'bson'})
    ds.setSendingState(task1, data1)
    gevent.sleep(1)
    dr = DataRequest(
        {'address': {'port': config['port'], 'host': config['host']}, 'taskId': task1, 'dataPath': '',
         'encoding': 'bson'})
    reply = dr.invoke()
    assert reply['data'] == data1
    ds.setSendingState(task2, data2)
    dr = DataRequest(
        {'address': {'port': config['port'], 'host': config['host']}, 'taskId': task2, 'dataPath': '',
         'encoding': 'bson'})
    reply = dr.invoke()
    assert reply ['data']== data2

def test_failing_to_get_data_old_task_id():
    ds = DataServer({'port': config['port'], 'encoding': 'bson'})
    ds.setSendingState(task1, data1)
    gevent.sleep(1)
    dr = DataRequest(
        {'address': {'port': config['port'], 'host': config['host']}, 'taskId': task1, 'dataPath': '',
         'encoding': 'bson'})
    reply = dr.invoke()
    assert reply['data'] == data1
    ds.setSendingState(task2, data2)
    dr = DataRequest(
        {'address': {'port': config['port'], 'host': config['host']}, 'taskId': task1, 'dataPath': '',
         'encoding': 'bson'})
    reply = dr.invoke()
    assert reply == {u'error': {u'message': u'Current taskId is task_2', u'code': u'notAvailable'}}

def test_failing_to_get_sending_ended():
    ds = DataServer({'port': config['port'], 'encoding': 'bson'})
    ds.setSendingState(task1, data1)
    gevent.sleep(1)
    dr = DataRequest(
        {'address': {'port': config['port'], 'host': config['host']}, 'taskId': task1, 'dataPath': '',
         'encoding': 'bson'})
    reply = dr.invoke()
    assert reply['data'] == data1
    ds.endSendingState()
    dr = DataRequest(
        {'address': {'port': config['port'], 'host': config['host']}, 'taskId': task1, 'dataPath': '',
         'encoding': 'bson'})
    reply = dr.invoke()
    assert reply == {u'error': {u'message': u'Current taskId is None', u'code': u'notAvailable'}}