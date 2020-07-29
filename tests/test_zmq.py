import gevent

from hkube_python_wrapper.communication.zmq.streaming.ZMQProducer import  ZMQProducer
from hkube_python_wrapper.communication.zmq.streaming.ZMQListener import ZMQListener

def test_queue():
    def doNothing(msg,customerType):
        return b'5'
    count = [0, 0, 0]
    producer = ZMQProducer(port=5556, maxMemorySize=5000, responseAcumulator=doNothing, consumerNames=['a','b'])
    gevent.spawn(producer.start)

    gevent.sleep()
    def doSomething(msg):
        count[0] = count[0] + 1
        gevent.sleep(0.1)
        return b'5'

    def doSomething2(msg):
        count[1] = count[1] + 1
        gevent.sleep(0.1)
        return b'5'

    def doSomething3(msg):
        count[2] = count[2] + 1
        gevent.sleep(0.1)
        return b'5'

    listener1 = ZMQListener('tcp://localhost:5556', doSomething,'a')
    listener2 = ZMQListener('tcp://localhost:5556', doSomething2,'b')
    listener3 = ZMQListener('tcp://localhost:5556', doSomething3,'a')
    gevent.spawn(listener1.start)
    gevent.spawn(listener2.start)
    gevent.spawn(listener3.start)
    gevent.sleep()
    producer.produce(b'bb1')
    producer.produce(b'bb2')
    producer.produce(b'bb3')
    producer.produce(b'bb4')
    producer.produce(b'bb5')
    gevent.sleep(3)
    producer.close()
    listener1.close()
    listener2.close()
    listener3.close()
    gevent.sleep(1)
    assert count[0] + count[1] + count[2] == 10

if __name__ == '__main__':
    test_queue()