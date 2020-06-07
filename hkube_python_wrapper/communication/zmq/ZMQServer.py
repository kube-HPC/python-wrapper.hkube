import threading
import zmq.green as zmq


class ZMQServer(threading.Thread):
    def __init__(self, context, replyFunc, workerUrl):
        self._isServing = False
        self._active = True
        self._socket = None
        self._replyFunc = replyFunc
        self._workerUrl = workerUrl
        self._context = context
        threading.Thread.__init__(self)

    def run(self):
        self._socket = self._context.socket(zmq.REP)
        self._socket.connect(self._workerUrl)

        while self._active:
            try:
                message = self._socket.recv()
                self._isServing = True
                self._send(message)
                self._isServing = False
            except Exception:
                print('socket closed')

    def _send(self, message):
        toBeSent = self._replyFunc(message)
        self._socket.send(toBeSent, copy=False)

    def isServing(self):
        return self._isServing

    def stop(self):
        self._active = False

    def close(self):
        self._socket.close()
