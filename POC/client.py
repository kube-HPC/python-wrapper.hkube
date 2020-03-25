
import zmq
from util.decorators import timing
from util.encoding import Encoding

context = zmq.Context()

encoding = Encoding('bson')


@timing
def invokeAdapter():
    socket = context.socket(zmq.REQ)
    socket.connect('tcp://localhost:9020')
    socket.send(b'hello')
    result = socket.recv()
    # result = encoding.decode(message)
    return result


# result = invokeAdapter()


@timing
def put(data, fileName):
    f = open(fileName, 'wb')
    f.write(data)
    f.close()


@timing
def get(fileName):
    f = open(fileName, 'rb')
    result = f.read()
    f.close()
    return result


sizes = [150, 500, 1000, 1500, 2000]

for size in sizes:
    fileName = 'file' + str(size)
    data = bytearray(b'\xdd'*(size*1000000))
    encoded = encoding.encode(data)
    put(encoded, fileName)
    fromFile = get(fileName)
    decoded = encoding.decode(fromFile)
    if(data == decoded):
        print('files are equals')
