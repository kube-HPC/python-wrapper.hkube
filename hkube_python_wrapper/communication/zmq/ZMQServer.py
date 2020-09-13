import time
import threading
import zmq.green as zmq


class ZMQServer(threading.Thread):
    def __init__(self, context, replyFunc, workerUrl):
        self._lastServing = None
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
                self._lastServing = time.time()
                if (message == b'Are you there'):
                    self._socket.send(b'Yes')
                else:
                    self._send(message)
                self._lastServing = time.time()
            except Exception as e:
                print(e)
                self._active = False

    def _send(self, message):
        try:
            toBeSent = self._replyFunc(message)
            self._socket.send_multipart(toBeSent, copy=False)
        except Exception as e:
            print(e)

    def isServing(self):
        return (self._lastServing) and (time.time() - self._lastServing < 10)

    def stop(self):
        self._active = False

    def close(self):
        self._socket.close()
