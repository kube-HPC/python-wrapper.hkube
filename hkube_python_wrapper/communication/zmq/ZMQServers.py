import zmq
import zmq.devices
from .ZMQServer import ZMQServer


class ZMQServers(object):
    def __init__(self, port, replyFunc):
        self._isServing = False
        self._active = True
        self._replyFunc = replyFunc
        self._url_worker = "inproc://workers"
        self._url_client = "tcp://*:" + str(port)
        self._instances = []
        self._device = None
        self._context = zmq.Context()
        self._context.setsockopt(zmq.LINGER, 0)

    def listen(self):

        for _ in range(5):
            server = ZMQServer(self._context, self._replyFunc, self._url_worker)
            server.start()
            self._instances.append(server)

        try:
            self._device = zmq.devices.ThreadDevice(zmq.QUEUE, zmq.ROUTER, zmq.DEALER)
            self._device.context_factory=lambda : self._context
            self._device.bind_in(self._url_client)
            self._device.setsockopt_in(zmq.LINGER, 0)
            self._device.bind_out(self._url_worker)
            self._device.setsockopt_out(zmq.LINGER, 0)
            self._device.start()
        except Exception as e:
            print('################################ zmq.device failed with '+str(e))


    def isServing(self):
        res = any(i.isServing() for i in self._instances)
        return res

    def close(self):
        print('closing zmq servers')

        for i in self._instances:
            print('closing zmq servers - stop')
            i.stop()
        print('joining zmq server threads')
        for i in self._instances:
            i.join()
        print('zmq context closing')
        self._context.term()
        print('zmq context closed')
        print('closed ZmqServers')
