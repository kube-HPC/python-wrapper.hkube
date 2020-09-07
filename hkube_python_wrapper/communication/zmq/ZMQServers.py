import zmq
import zmq.devices
from .ZMQServer import ZMQServer
context = zmq.Context()


class ZMQServers(object):
    def __init__(self, port, replyFunc):
        self._isServing = False
        self._active = True
        self._replyFunc = replyFunc
        self._url_worker = "inproc://workers"
        self._url_client = "tcp://*:" + str(port)
        self._instances = []
        self._device = None

    def listen(self):
        # self._clients = context.socket(zmq.ROUTER)
        # self._clients.bind(self._url_client)

        # self._workers = context.socket(zmq.DEALER)
        # self._workers.bind(self._url_worker)

        for _ in range(5):
            server = ZMQServer(context, self._replyFunc, self._url_worker)
            server.start()
            self._instances.append(server)

        try:
            # zmq.device(zmq.QUEUE, self._clients, self._workers)
            self._device = zmq.devices.ThreadProxy(zmq.QUEUE, zmq.ROUTER, zmq.DEALER)
            self._device.bind_in(self._url_client)
            self._device.setsockopt_in(zmq.LINGER, 0)
            self._device.bind_out(self._url_worker)
            self._device.setsockopt_out(zmq.LINGER, 0)
            self._device.start()
        except Exception as e:
            print('################################ zmq.device failed with '+str(e))

        # self._clients.close()
        # self._workers.close()

    def isServing(self):
        res = any(i.isServing() for i in self._instances)
        return res

    def close(self):
        print('closing zmq servers')
        for i in self._instances:
            print('closing zmq servers - stop')
            i.stop()
            print('closing zmq servers - close')
            i.close()
        # context.term()
        # for i in self._instances:
        #     print('closing zmq servers - join')
        #     i.join()
        #     print('closing zmq servers - thats all')
        if (self._device):
            self._device._context.term() # pylint: disable=protected-access
            self._device.join()
        print('closed ZmqServers')
