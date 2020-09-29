import zmq
import time
from .consts import consts

context = zmq.Context()


class ZMQRequest(object):
    def __init__(self, reqDetails):
        self.socket, self.connStr = self._createSocket(host=reqDetails['host'], port=reqDetails['port'])
        self.ping_socket, self.pingConnStr = self._createSocket(host=reqDetails['host'], port=int(reqDetails['port'])+1)
        self.content = reqDetails['content']
        self.timeout = int(reqDetails['timeout'])
        self.networkTimeout = int(reqDetails['networkTimeout'])
        self.pingTime = 1e10

    def invokeAdapter(self):
        print('sending ping')
        pingStart = time.time()
        self.ping_socket.send(consts.zmq.ping)
        result = self.ping_socket.poll(self.networkTimeout)
        if (result):
            there = self.ping_socket.recv()
            if (there == consts.zmq.pong):
                print('got pong')
                pingEnd = time.time()
                self.pingTime = (pingEnd-pingStart)*1000
                self.socket.send(self.content)
                result = self.socket.poll(self.timeout)
                if (result):
                    message = self.socket.recv_multipart()
                    return message
                print('Timed out')
                raise Exception('Timed out:' + str(self.timeout))
        print('Ping timed out ({timeout}) to {conn}'.format(timeout=self.networkTimeout, conn=self.pingConnStr))
        raise Exception('Ping timed out ({timeout}) to {conn}'.format(timeout=self.networkTimeout, conn=self.pingConnStr))

    def close(self):
        self.socket.close()
        self.ping_socket.close()

    def _createSocket(self, host, port):
        connStr = 'tcp://' + host + ':' + str(port)
        print('creating socket to: {s}'.format(s=connStr))
        socket = context.socket(zmq.REQ)
        socket.setsockopt(zmq.LINGER, 0)
        socket.connect(connStr)
        return socket, connStr
