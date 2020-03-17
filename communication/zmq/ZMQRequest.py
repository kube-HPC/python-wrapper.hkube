
import zmq.green as zmq

import gevent

context = zmq.Context()


class ZMQRequest(object):
    def __init__(self, reqDetails):
        self.socket = context.socket(zmq.REQ)
        self.socket.connect('tcp://'+reqDetails['host']+':'+str(reqDetails['port']))
        self.connected = False
        self.content = reqDetails['content']

    def invoke(self):
        self.socket.send(self.content)
        # gevent.sleep(1)
        message = self.socket.recv()
        return message