import threading
import zmq
from .consts import consts


class ZMQServer(threading.Thread):
    def __init__(self, context, replyFunc, workerUrl):
        self._isServing = False
        self._active = True
        self._socket = None
        self._replyFunc = replyFunc
        self._workerUrl = workerUrl
        self._context = context
        threading.Thread.__init__(self)
        self.daemon = True

    def run(self):
        self._socket = self._context.socket(zmq.REP)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.connect(self._workerUrl)

        while self._active:
            try:
                events = self._socket.poll(timeout=1000)
                if (events == 0):
                    continue
                message = self._socket.recv()
                self._isServing = True
                if(message == consts.zmq.ping):
                    self._socket.send(consts.zmq.pong)
                else:
                    self._send(message)
                self._isServing = False
            except Exception as e:
                print('socket closed: '+str(e))
                break
        print('ZmqServer run loop exit')
        self._socket.close()

    def _send(self, message):
        toBeSent = self._replyFunc(message)
        self._socket.send(toBeSent, copy=False)

    def isServing(self):
        return self._isServing

    def stop(self):
        self._active = False

    def close(self):
        self._socket.close()
