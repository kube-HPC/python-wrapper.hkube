import zmq.green as zmq
from gevent import spawn


class ZMQServer(object):
    def __init__(self, config, getReplyFunc):
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)
        self.socket.bind("tcp://*:" + str(config['port']))
        self.getReplyFunc = getReplyFunc
        self.serving = False

        def listen(server):
            while True:
                message = server.socket.recv()
                self.serving = True
                server.socket.send(self.getReplyFunc(message))
                self.serving = False

        spawn(listen, (self))

    def isServing(self):
        return self.serving

    def close(self):
        self.socket.close()
