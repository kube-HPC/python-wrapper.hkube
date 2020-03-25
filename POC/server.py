import zmq
from util.encoding import Encoding
from util.decorators import timing

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:" + '9020')

encoding = Encoding('bson')
size = 500
# data = bytearray(5)
# data = bytearray(500 * 1000000)


def listen():
    while True:
        message = socket.recv()
        print('receive msg from client')
        # result = encoding.encode(data)
        socket.send(result)


listen()
