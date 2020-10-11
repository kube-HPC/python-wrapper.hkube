import zmq
from .consts import consts
from hkube_python_wrapper.util.decorators import timing

context = zmq.Context()


class ZMQRequest(object):
    def __init__(self, reqDetails):
        self.socket, self.connStr = self._createSocket(host=reqDetails['host'], port=reqDetails['port'])
        self.ping_socket, self.pingConnStr = self._createSocket(host=reqDetails['host'], port=int(reqDetails['port'])+1)
        self.content = reqDetails['content']
        self.timeout = int(reqDetails['timeout'])
        self.networkTimeout = int(reqDetails['networkTimeout'])

    def invokeAdapter(self):
        pong = self.ping()
        message = None
        if (pong):
            message = self.request()
        return message

    @timing
    def request(self):
        self.socket.send(self.content)
        result = self.socket.poll(self.timeout)
        if (result):
            message = self.socket.recv_multipart()
            return message
        err = 'request timed out ({timeout}) to {conn}'.format(timeout=self.timeout, conn=self.connStr)
        print(err)
        raise Exception(err)

    @timing
    def ping(self):
        self.ping_socket.send(consts.zmq.ping)
        result = self.ping_socket.poll(self.networkTimeout)
        if (result):
            message = self.ping_socket.recv()
            if (message == consts.zmq.pong):
                return True
        err = 'ping timed out ({timeout}) to {conn}'.format(timeout=self.networkTimeout, conn=self.pingConnStr)
        print(err)
        raise Exception(err)

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
