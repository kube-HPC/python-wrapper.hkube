
import zmq

context = zmq.Context()


class ZMQRequest(object):
    def __init__(self, reqDetails):
        self.poller = zmq.Poller()
        self.socket = context.socket(zmq.REQ)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.connect('tcp://' + reqDetails['host'] + ':' + str(reqDetails['port']))
        self.poller.register(self.socket, zmq.POLLIN)
        self.content = reqDetails['content']
        self.timeout = int(reqDetails['timeout'])

    def invokeAdapter(self):
        self.socket.send(self.content)
        result = self.poller.poll(self.timeout)
        if (result):
            message = self.socket.recv()
            return message
        raise Exception('Timed out:' + str(self.timeout))

    def close(self):
        self.socket.close()
