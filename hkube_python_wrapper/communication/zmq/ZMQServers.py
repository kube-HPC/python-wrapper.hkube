import zmq.green as zmq
from .ZMQServer import ZMQServer
context = zmq.Context()


class ZMQServers(object):
    def __init__(self, port, replyFunc, num_threads):
        self._isServing = False
        self._active = True
        self._replyFunc = replyFunc
        self._url_worker = "inproc://workers"
        self._url_client = "tcp://*:" + str(port)
        self._instances = []
        self._clients = None
        self._workers = None
        self._num_threads = num_threads

    def listen(self):
        self._clients = context.socket(zmq.ROUTER)
        self._clients.bind(self._url_client)

        self._workers = context.socket(zmq.DEALER)
        self._workers.bind(self._url_worker)
        print('Creating {num_threads} ZMQ Servers'.format(num_threads=self._num_threads))
        for _ in range(self._num_threads):
            server = ZMQServer(context, self._replyFunc, self._url_worker)
            server.start()
            self._instances.append(server)

        zmq.device(zmq.QUEUE, self._clients, self._workers)

        self._clients.close()
        self._workers.close()
        context.term()

    def isServing(self):
        res = any(i.isServing() for i in self._instances)
        return res

    def close(self):
        for i in self._instances:
            i.stop()
            i.close()
            i.join(timeout=1)
        self._clients.close()
        self._workers.close()
        context.term()
