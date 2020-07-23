import time
from threading import Thread

from hkube_python_wrapper.communication.zmq import ZMQListener, ZMQPublisher


def test_queue():

    count = [0, 0, 0]
    publisher = ZMQPublisher.ZMQPublisher(port=5556, maxMemorySize=5000)
    thread = Thread(target=publisher.start)
    thread.start()
    time.sleep(3)
    def doSomething(msg):
        count[0] = count[0] + 1
        time.sleep(0.1)

    def doSomething2(msg):
        count[1] = count[1] + 1
        time.sleep(0.1)

    def doSomething3(msg):
        count[2] = count[2] + 1
        time.sleep(0.1)

    listener1 = ZMQListener.ZMQListener('tcp://localhost:5556', doSomething)
    listener2 = ZMQListener.ZMQListener('tcp://localhost:5556', doSomething2)
    listener3 = ZMQListener.ZMQListener('tcp://localhost:5556', doSomething3)
    thread = Thread(target=listener1.start)
    thread.start()
    thread = Thread(target=listener2.start)
    thread.start()
    thread = Thread(target=listener3.start)
    thread.start()
    time.sleep(5)

    publisher.send(b'bb1')
    publisher.send(b'bb2')
    publisher.send(b'bb3')
    publisher.send(b'bb4')
    publisher.send(b'bb5')
    time.sleep(1)
    publisher.close()
    listener1.close()
    listener2.close()
    listener3.close()
    time.sleep(1)
    assert count[0] + count[1] + count[2] == 5

