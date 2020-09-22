import zmq
import zmq.devices
import os
import multiprocessing

from .ZMQServer import ZMQServer
from .ZMQPingServer import ZMQPingServer

class ZMQServers(object):
    def __init__(self, port, replyFunc, num_threads):
        self._isServing = False
        self._active = True
        self._replyFunc = replyFunc
        self._url_worker = "inproc://workers"
        self._url_client = "tcp://*:" + str(port)
        self._instances = []
        self._port = int(port)
        self._device = None
        self._context = zmq.Context()
        self._context.setsockopt(zmq.LINGER, 0)
        self._num_threads = num_threads
        self._num_ping_threads = 10

    def listen(self):
        pingProcess = multiprocessing.Process(target=self._createZmqPingServers, args=(self._port,), name="Ping Servers Process")
        pingProcess.daemon = True
        pingProcess.start()
        for _ in range(self._num_threads):
            server = ZMQServer(self._context, self._replyFunc, self._url_worker)
            server.start()
            self._instances.append(server)

        try:
            self._device = zmq.devices.ThreadDevice(zmq.QUEUE, zmq.ROUTER, zmq.DEALER)
            self._device.context_factory = lambda: self._context
            self._device.bind_in(self._url_client)
            self._device.setsockopt_in(zmq.LINGER, 0)
            self._device.bind_out(self._url_worker)
            self._device.setsockopt_out(zmq.LINGER, 0)
            self._device.start()
        except Exception as e:
            print('zmq.device failed with '+str(e))
            raise

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

    def _createZmqPingServers(self, port):
        try:
            pingContext = zmq.Context()
            url_worker = "inproc://ping_workers"
            url_client = "tcp://*:" + str(port+1)

            def createDealerRouter(url_client, url_worker, context):
                clients = context.socket(zmq.ROUTER)
                clients.bind(url_client)
                workers = context.socket(zmq.DEALER)
                workers.bind(url_worker)
                return (clients, workers)

            clients, workers = createDealerRouter(url_client=url_client, url_worker=url_worker, context=pingContext)
            print('clients: {c}, workers: {w}'.format(c=clients.get(zmq.LAST_ENDPOINT), w=workers.get(zmq.LAST_ENDPOINT)))
            print('Creating {num_threads} ZMQ Ping Servers on port {port}. pid: {pid}'.format(port=url_client, num_threads=self._num_ping_threads, pid=os.getpid()))
            for i in range(self._num_ping_threads):
                server = ZMQPingServer(pingContext, url_worker, 'Ping-Thread-'+str(i))
                server.start()
            zmq.device(zmq.QUEUE, clients, workers)
        except Exception as e:
            print(e)
