import pytest
import zmq
from gevent import spawn,sleep
from communication.DataRequest import DataRequest
from communication.DataServer import DataServer

import gevent

reources = {}
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
data3 = bytearray(1024 * 1024 * 4000)
def run():
    ds = DataServer({'port': config['port'], 'host': config['host'], 'encoding': 'bson'})


    def sleepNow(message):
        sleep(3)
        return ds.cr(message)

    ds.cr = ds._createReply
    ds._createReply = sleepNow
    ds.setSendingState(task1, data2)
    def printServing():
        print (str(ds.isServing()))
        sleep(1)
        printServing()
    spawn(printServing)
    ds.listen()
    gevent.sleep(3000)


if __name__ == '__main__':
    run()