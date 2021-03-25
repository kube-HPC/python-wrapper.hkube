import time
from hkube_python_wrapper.util.DaemonThread import DaemonThread
from hkube_python_wrapper.util.logger import log
import zmq
from .consts import consts


class ZMQServer(DaemonThread):
    def __init__(self, context, replyFunc, workerUrl):
        self._lastServing = None
        self._active = True
        self._socket = None
        self._replyFunc = replyFunc
        self._workerUrl = workerUrl
        self._context = context
        DaemonThread.__init__(self)

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
                self._lastServing = time.time()
                if(message == consts.zmq.ping):
                    self._socket.send(consts.zmq.pong)
                else:
                    self._send(message)
                self._lastServing = time.time()
            except Exception as e:
                log.error('socket closed: {e}', e=str(e))
                break
        self.close()

    def _send(self, message):
        try:
            toBeSent = self._replyFunc(message)
            self._socket.send_multipart(toBeSent, copy=False)
        except Exception as e:
            log.error(e)

    def isServing(self):
        return (self._lastServing) and (time.time() - self._lastServing < 10)

    def stop(self):
        self._active = False

    def close(self):
        self._socket.close()
