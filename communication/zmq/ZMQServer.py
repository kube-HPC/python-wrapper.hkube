import time
import zmq
# import thread


class ZMQServer(object):
    def __init__(self, config, getReplyFunc):
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)
        self.socket.bind("tcp://*:" + str(config['port']))
        self.getReplyFunc = getReplyFunc

        # def listen(server):
        #     while True:
        #     # Wait for next request from client
        #         message = server.socket.recv()
        #         server.socket.send(getReplyFunc(message))
        # thread.start_new_thread(listen,({self}))
