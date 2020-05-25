import threading
import zmq.green as zmq
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

    def listen(self):
        clients = context.socket(zmq.ROUTER)
        clients.bind(self._url_client)

        workers = context.socket(zmq.DEALER)
        workers.bind(self._url_worker)

        for i in range(5):
            server = ZMQServer(context, self._replyFunc, self._url_worker)
            server.start()
            self._instances.append(server)

        zmq.device(zmq.QUEUE, clients, workers)

        clients.close()
        workers.close()
        context.term()

    def isServing(self):
        res = all(i.isServing() for i in self._instances)
        return res

    def stop(self):
        for i in self._instances:
            i.stop()

    def close(self):
        for i in self._instances:
            i.close()
