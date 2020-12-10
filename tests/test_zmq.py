import time
from threading import Thread

from hkube_python_wrapper.communication.zmq.streaming.producer.ZMQProducer import ZMQProducer
from hkube_python_wrapper.communication.zmq.streaming.ZMQListener import ZMQListener
from hkube_python_wrapper.util.encoding import Encoding

PROTOCOL_TYPE_MSGPACK = 0x03
DATA_TYPE_RAW = 0x01
encoding = Encoding('msgpack')
header = encoding.createHeader(DATA_TYPE_RAW, PROTOCOL_TYPE_MSGPACK)


def test_zmq_streaming():
    def doNothing(header, msg):
        return b'5'

    count = [0, 0]
    producer = ZMQProducer(port=5556, maxMemorySize=5000, responseAcumulator=doNothing, consumerTypes=['b', 'c'], encoding=encoding, me='a')
    runThread = Thread(name="Producer", target=producer.start)
    runThread.start()

    time.sleep(1)

    def doSomething(env, header, msg):
        count[0] = count[0] + 1
        time.sleep(0.1)
        return b'5'

    def doSomething2(env, header, msg):
        count[1] = count[1] + 1
        time.sleep(0.1)
        return b'5'

    listener1 = ZMQListener('tcp://localhost:5556', doSomething, encoding, 'b')
    listener2 = ZMQListener('tcp://localhost:5556', doSomething2, encoding, 'c')
    runThread = Thread(name="Listener1", target=listener1.start)
    runThread.start()
    runThread = Thread(name="Listener2", target=listener2.start)
    runThread.start()
    time.sleep(4)
    env = [{
        "source": "a",
        "next": [
            "b", "c"
        ]
    }]
    producer.produce(header, b'bb1', env)
    producer.produce(header, b'bb2', env)
    producer.produce(header, b'bb3', env)
    producer.produce(header, b'bb4', env)
    producer.produce(header, b'bb5', env)
    time.sleep(3)

    listener1.close()
    listener2.close()
    time.sleep(1)
    producer.close()
    print(str(count[0]))
    print(str(count[1]))

    assert count[0] + count[1] == 10


if __name__ == '__main__':
    test_zmq_streaming()
