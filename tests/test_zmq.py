import time
import gevent

from hkube_python_wrapper.communication.zmq import ZMQListener, ZMQPublisher


def test_queue():

    count = [0, 0, 0]
    publisher = ZMQPublisher.ZMQPublisher(port=5556, maxMemorySize=5000)
    gevent.spawn(publisher.start)

    gevent.sleep()
    def doSomething(msg):
        count[0] = count[0] + 1
        gevent.sleep(0.1)

    def doSomething2(msg):
        count[1] = count[1] + 1
        gevent.sleep(0.1)

    def doSomething3(msg):
        count[2] = count[2] + 1
        gevent.sleep(0.1)

    listener1 = ZMQListener.ZMQListener('tcp://localhost:5556', doSomething)
    listener2 = ZMQListener.ZMQListener('tcp://localhost:5556', doSomething2)
    listener3 = ZMQListener.ZMQListener('tcp://localhost:5556', doSomething3)
    gevent.spawn(listener1.start)
    gevent.spawn(listener2.start)
    gevent.spawn(listener3.start)
    gevent.sleep()
    publisher.send(b'bb1')
    publisher.send(b'bb2')
    publisher.send(b'bb3')
    publisher.send(b'bb4')
    publisher.send(b'bb5')
    gevent.sleep(1)
    publisher.close()
    listener1.close()
    listener2.close()
    listener3.close()
    gevent.sleep(1)
    assert count[0] + count[1] + count[2] == 5

