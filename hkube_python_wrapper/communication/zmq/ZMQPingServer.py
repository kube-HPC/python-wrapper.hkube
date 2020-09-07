import threading
import zmq.green as zmq


class ZMQPingServer(threading.Thread):
    def __init__(self, context, workerUrl, name):
        self._isServing = False
        self._active = True
        self._socket = None
        self._workerUrl = workerUrl
        self._context = context
        threading.Thread.__init__(self, name=name)

    def run(self):
        self._socket = self._context.socket(zmq.REP)
        self._socket.connect(self._workerUrl)

        while self._active:
            try:
                print('waiting for ping on {p}'.format(p=self._socket.get(zmq.LAST_ENDPOINT)))
                message = self._socket.recv()
                self._isServing = True
                print('got ping')
                if(message == b'ping'):
                    self._socket.send(b'pong')
                self._isServing = False
            except Exception:
                print('socket closed')

    def isServing(self):
        return self._isServing

    def stop(self):
        self._active = False

    def close(self):
        self._socket.close()
