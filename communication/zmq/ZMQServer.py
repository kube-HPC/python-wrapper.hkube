import zmq.green as zmq
from gevent import spawn



class ZMQServer(object):
    def __init__(self, config, getReplyFunc):
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)
        self.socket.bind("tcp://*:" + str(config['port']))
        self.getReplyFunc = getReplyFunc

        def listen(server):
            while True:
                # Wait for next request from client
                try:
                    message = server.socket.recv()
                    server.socket.send(getReplyFunc(message))
                except Exception as e:
                    print (str(e))
                    server.socket.close()
                    context = zmq.Context()
                    server.socket = context.socket(zmq.REP)
                    server.socket.bind("tcp://*:" + str(config['port']))

        spawn(listen, (self))
