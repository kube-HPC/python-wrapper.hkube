import zmq.green as zmq

context = zmq.Context()


class ZMQRequest(object):
    def __init__(self, reqDetails):
        self.socket, self.poller = self._createSocket(host=reqDetails['host'], port=reqDetails['port'])
        self.ping_socket, self.ping_poller = self._createSocket(host=reqDetails['host'], port=int(reqDetails['port'])+1)
        self.content = reqDetails['content']
        self.timeout = int(reqDetails['timeout'])
        self.networkTimeout = int(reqDetails['networkTimeout'])

    def invokeAdapter(self):
        print('sending ping to: {s}'.format(s=self.ping_socket.get(zmq.LAST_ENDPOINT)))
        self.ping_socket.send(b'ping')
        result = self.ping_poller.poll(self.networkTimeout)
        if (result):
            there = self.ping_socket.recv()
            if (there == b'pong'):
                self.socket.send(self.content)
                result = self.poller.poll(self.timeout)
                if (result):
                    message = self.socket.recv()
                    return message
                raise Exception('Timed out:' + str(self.timeout))
        raise Exception('No server on other side. Ping timed out: '+ str(self.networkTimeout))

    def _createSocket(self, host, port):
        connStr = 'tcp://' + host + ':' + str(port)
        print('creating socket to: {s}'.format(s=connStr))
        poller = zmq.Poller()
        socket = context.socket(zmq.REQ)
        socket.setsockopt(zmq.LINGER, 0)
        socket.connect(connStr)
        poller.register(socket, zmq.POLLIN)
        return (socket, poller)

    def close(self):
        self.socket.close()
