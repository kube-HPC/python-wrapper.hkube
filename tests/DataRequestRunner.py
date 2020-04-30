import pytest
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
def run():
    dr = DataRequest(
        {'address': {'port': config['port'], 'host': config['host']}, 'taskId': task1, 'dataPath': '',
         'encoding': 'bson'})
    res = dr.invoke()
    print (res)
    gevent.sleep(5)
if __name__ == '__main__':
    run()