from hkube_python_wrapper.communication.streaming.MessageListener import MessageListener
from hkube_python_wrapper.communication.streaming.MessageProducer import MessageProducer
import gevent

producer_config = {'port': 5557, 'messageMemoryBuff': 5000, 'encoding': 'msgpack', 'statisticsInterval': 0.1}
listenr_config = {'remoteAddress': 'tcp://localhost:5557', 'encoding': 'msgpack'}


def test_Messaging():
    asserts = {}
    messageProducer = MessageProducer(producer_config, ['a'])


    def onStatistics(statistics):
        asserts['stats'] = statistics

    messageProducer.registerStatisticsListener(onStatistics)
    gevent.sleep(3)

    def onMessage(msg):
        print("\n In onMessage\n")
        asserts['field1'] = msg['field1']
        gevent.sleep(1)
    gevent.spawn(messageProducer.start)
    gevent.spawn(messageProducer.start)
    messageProducer.produce({'field1': 'value1'})
    messageProducer.produce({'field1': 'value1'})
    messageProducer.produce({'field1': 'value1'})
    gevent.sleep(0.5)
    assert asserts['stats'][0]['queueSize'] == 3
    assert asserts['stats'][0]['sent'] == 0
    assert len(asserts['stats'][0]['durationList']) == 0
    messageListener = MessageListener(listenr_config, consumerType='a')
    messageListener.registerMessageListener(onMessage)
    gevent.spawn(messageListener.start)
    gevent.sleep(4.2)
    assert asserts['field1'] == 'value1'
    assert asserts['stats'][0]['queueSize'] == 0
    assert asserts['stats'][0]['sent'] == 3
    assert len(asserts['stats'][0]['durationList']) == 3
    assert messageProducer.getMessageProcessTime('a') >= 1
    gevent.sleep()
    messageProducer.close()
    messageListener.close()
