import time
from threading import Thread

from hkube_python_wrapper.communication.zmq.streaming.ZMQProducer import  ZMQProducer
from hkube_python_wrapper.communication.zmq.streaming.ZMQListener import ZMQListener

def test_queue():
    def doNothing(msg,customerType):
        return b'5'
    count = [0, 0, 0]
    producer = ZMQProducer(port=5556, maxMemorySize=5000, responseAcumulator=doNothing, consumerTypes=['a', 'b'])
    runThread = Thread(name="Producer", target=producer.start)
    runThread.start()

    time.sleep(1)
    def doSomething(msg):
        count[0] = count[0] + 1
        time.sleep(0.1)
        return b'5'

    def doSomething2(msg):
        count[1] = count[1] + 1
        time.sleep(0.1)
        return b'5'

    def doSomething3(msg):
        count[2] = count[2] + 1
        time.sleep(0.1)
        return b'5'

    listener1 = ZMQListener('tcp://localhost:5556', doSomething,'a')
    listener2 = ZMQListener('tcp://localhost:5556', doSomething2,'b')
    listener3 = ZMQListener('tcp://localhost:5556', doSomething3,'a')
    runThread = Thread(name="Listener1", target=listener1.start)
    runThread.start()
    runThread = Thread(name="Listener2", target=listener2.start)
    runThread.start()
    runThread = Thread(name="Listener3", target=listener3.start)
    runThread.start()
    time.sleep(1)
    producer.produce(b'bb1')
    producer.produce(b'bb2')
    producer.produce(b'bb3')
    producer.produce(b'bb4')
    producer.produce(b'bb5')
    time.sleep(3)
    producer.close()
    listener1.close()
    listener2.close()
    listener3.close()
    time.sleep(1)
    assert count[0] + count[1] + count[2] == 10

if __name__ == '__main__':
    test_queue()