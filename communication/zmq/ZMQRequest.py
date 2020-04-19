
import zmq
from util.decorators import timing
context = zmq.Context()

class ZMQRequest(object):
    def __init__(self, reqDetails):
        self.socket = context.socket(zmq.REQ)
        self.socket.connect('tcp://'+reqDetails['host']+':'+str(reqDetails['port']))
        self.connected = False
        self.content = reqDetails['content']

    @timing
    def invokeAdapter(self):
        self.socket.send(self.content)
        message = self.socket.recv(copy=False)
        return message
