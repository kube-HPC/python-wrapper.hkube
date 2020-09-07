import zmq.green as zmq
from .ZMQServer import ZMQServer
from .ZMQPingServer import ZMQPingServer
import os
import threading
import multiprocessing
from collections import namedtuple
ServerData = namedtuple('ServerData', 'device clients workers')

globalContext = zmq.Context()


class ZMQServers(object):
    def __init__(self, port, replyFunc, num_threads):
        self._isServing = False
        self._active = True
        self._replyFunc = replyFunc
        self._port = int(port)
        self._pingServers=None
        self._servers=None
        self._instances = []
        self._clients = None
        self._workers = None
        self._num_threads = num_threads
        self._num_ping_threads = 10

    def _createDealerRouter(self, url_client, url_worker, context):
        clients = context.socket(zmq.ROUTER)
        clients.bind(url_client)

        workers = context.socket(zmq.DEALER)
        workers.bind(url_worker)
        return (clients, workers)

    def _createZmqServers(self, port):
        url_worker = "inproc://workers"
        url_client = "tcp://*:" + str(port)
        clients, workers = self._createDealerRouter(url_client=url_client, url_worker=url_worker,context=globalContext)
        print('Creating {num_threads} ZMQ Servers'.format(num_threads=self._num_threads))
        for _ in range(self._num_threads):
            server = ZMQServer(globalContext, self._replyFunc, url_worker)
            server.start()
            self._instances.append(server)
        device = threading.Thread(target=zmq.device, args=(zmq.QUEUE, clients, workers))
        device.start()
        return ServerData(device, clients, workers)

    def _createZmqPingServers(self, port):
        try:
            pingContext=zmq.Context()
            url_worker = "inproc://ping_workers"
            url_client = "tcp://*:" + str(port+1)
            def createDealerRouter(url_client, url_worker, context):
                clients = context.socket(zmq.ROUTER)
                clients.bind(url_client)

                workers = context.socket(zmq.DEALER)
                workers.bind(url_worker)
                return (clients, workers)
            clients, workers = createDealerRouter(url_client=url_client, url_worker=url_worker,context=pingContext)
            print('clients: {c}, workers: {w}'.format(c=clients.get(zmq.LAST_ENDPOINT), w=workers.get(zmq.LAST_ENDPOINT)))
            print('Creating {num_threads} ZMQ Ping Servers on port {port}. pid: {pid}'.format(port=url_client,num_threads=self._num_ping_threads, pid=os.getpid()))
            for i in range(self._num_ping_threads):
                server = ZMQPingServer(pingContext, url_worker, 'Ping-Thread-'+str(i))
                server.start()
                # self._instances.append(server)
            # ping_device = threading.Thread(target=zmq.device, args=(zmq.QUEUE, clients, workers))
            # ping_device.start()
            # return ServerData(ping_device, clients, workers)
            zmq.device(zmq.QUEUE, clients, workers)
        except Exception as e:
            print(e)
       

    def listen(self):
        self._servers = self._createZmqServers(self._port)
        # pingProcess = multiprocessing.Process(target=self._createZmqPingServers,args=(self._port,))
        # pingProcess.start()
        self._servers.device.join()
        for s in [self._servers]:
            s.clients.close()
            s.workers.close()
        globalContext.term()

    def isServing(self):
        res = any(i.isServing() for i in self._instances)
        return res

    def close(self):
        for i in self._instances:
            i.stop()
            i.close()
            i.join(timeout=1)
        for s in [self._servers]:
            if (s):
                s.clients.close()
                s.workers.close()
        globalContext.term()
