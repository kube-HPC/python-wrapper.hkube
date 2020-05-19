import zmq.green as zmq
from gevent import spawn
context = zmq.Context()


class ZMQServer(object):
    def __init__(self):
        self._isServing = False
        self._active = True
        self._createReplyFunc = None
        self._socket = None

    def listen(self, port, getReplyFunc):
        self._createReplyFunc = getReplyFunc
        self._socket = context.socket(zmq.REP)
        self._socket.bind("tcp://*:" + str(port))

        def onReceive():
            while self._active:
                message = self._socket.recv()
                self._isServing = True
                self.send(message)
                self._isServing = False

        spawn(onReceive)

    def send(self, message):
        toBeSent = self._createReplyFunc(message)
        self._socket.send(toBeSent, copy=False)

    def isServing(self):
        return self._isServing

    def stop(self):
        self._active = False

    def close(self):
        self._socket.close()
