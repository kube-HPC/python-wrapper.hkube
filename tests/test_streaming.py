from hkube_python_wrapper.communication.streaming.MessageListener import MessageListener
from hkube_python_wrapper.communication.streaming.MessageProducer import MessageProducer
import gevent

producer_config = {'port': 5556, 'messageMemoryBuff': 5000, 'encoding': 'msgpack'}
listenr_config = {'remoteAddress': 'tcp://localhost:5556', 'encoding': 'msgpack'}


def test_Messaging():
    messageProducer = MessageProducer(producer_config)
    asserts = {}

    def onMessage(msg):
        asserts['field1'] = msg['field1']
        gevent.sleep(1)

    gevent.spawn(messageProducer.start)
    messageListener = MessageListener(listenr_config, onMessage=onMessage)
    gevent.spawn(messageListener.start)
    gevent.spawn(messageProducer.start)
    messageProducer.produce({'field1': 'value1'})
    messageProducer.produce({'field1': 'value1'})
    gevent.sleep(2.2)
    assert asserts['field1'] == 'value1'
    assert messageProducer.getMessageProcessTime() >= 1
    gevent.sleep()
    messageProducer.close()
    messageListener.close()
