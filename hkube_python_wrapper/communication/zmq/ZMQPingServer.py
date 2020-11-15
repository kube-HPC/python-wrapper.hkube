import zmq
from .consts import consts
from hkube_python_wrapper.util.DaemonThread import DaemonThread

class ZMQPingServer(DaemonThread):
    def __init__(self, context, workerUrl, name):
        self._active = True
        self._socket = None
        self._workerUrl = workerUrl
        self._context = context
        DaemonThread.__init__(self, name=name)

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
                if(message == consts.zmq.ping):
                    self._socket.send(consts.zmq.pong)
            except Exception as e:
                print('socket closed: '+str(e))
                break
        print('ZmqPingServer run loop exit')
        self.close()

    def stop(self):
        self._active = False

    def close(self):
        self._socket.close()
