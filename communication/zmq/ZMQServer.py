import zmq.green as zmq
context = zmq.Context()


class ZMQServer(object):
    def __init__(self):
        self._active = True
        self._serving = False
        self._getReplyFunc = None
        self._socket = None

    def listen(self, port, getReplyFunc):
        self._getReplyFunc = getReplyFunc
        self._socket = context.socket(zmq.REP)
        self._socket.bind("tcp://*:" + str(port))

        while self._active:
            message = self._socket.recv()
            self._serving = True
            self._socket.send(self._getReplyFunc(message))
            self._serving = False

    def isServing(self):
        return self._serving

    def stop(self):
        self._active = False

    def close(self):
        self._socket.close()
