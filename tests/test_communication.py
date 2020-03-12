from communication.DataRequest import DataRequest
from communication.DataServer import DataServer

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
}
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
    try:
        ds = DataServer({'port': config['port'], 'encoding': 'bson'})
        ds.setSendingState(task1, data1)
        dr = DataRequest(
            {'address': {'port': config['port'],
                         'host': config['host']},
             'taskId': task1, 'dataPath': 'level1', 'encoding': 'bson'})
        reply = dr.invoke()
        assert reply == {
            'level2': {
                'value1': 'l1_l2_value_1',
                'value2': 'l1_l2_value_2',
            },
            'value1': 'l1_value_1'
        }

    except Exception as e:
        print(e)


def test_get_complete_data():
    try:
        ds = DataServer({'port': config['port'], 'encoding': 'bson'})
        ds.setSendingState(task1, data1)
        dr = DataRequest(
            {'address': {'port': config['port'],
                         'host': config['host']},
             'taskId': task1, 'dataPath': '', 'encoding': 'bson'})
        reply = dr.invoke()
        assert reply == {
            'level2': {
                'value1': 'l1_l2_value_1',
                'value2': 'l1_l2_value_2',
            },
            'value1': 'l1_value_1'
        }
    except Exception as e:
        print(e)
