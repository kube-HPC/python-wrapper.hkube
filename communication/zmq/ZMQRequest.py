
import zmq
context = zmq.Context()


class ZMQRequest(object):
    def __init__(self, reqDetails):
        self.socket = context.socket(zmq.REQ)
        self.socket.connect('tcp://'+reqDetails['host']+':'+str(reqDetails['port']))
        self.connected = False
        self.content = reqDetails['content']

    def invokeAdapter(self):
        self.socket.send(self.content)
        message = self.socket.recv()
        return message
