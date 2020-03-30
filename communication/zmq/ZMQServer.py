import zmq.green as zmq
from gevent import spawn
context = zmq.Context()


class ZMQServer(object):
    def __init__(self):
        self._active = True
        self.serving = False

    def listen(self, port, getReplyFunc):
        self.getReplyFunc = getReplyFunc
        self.socket = context.socket(zmq.REP)
        self.socket.bind("tcp://*:" + str(port))

        while self._active:
            message = self.socket.recv()
            self.serving = True
            self.socket.send(self.getReplyFunc(message))
            self.serving = False

    def isServing(self):
        return self.serving

    def stop(self):
        self._active = False

    def close(self):
        self.socket.close()
