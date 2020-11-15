from hkube_python_wrapper.communication.streaming.MessageListener import MessageListener
from hkube_python_wrapper.communication.streaming.MessageProducer import MessageProducer
import time
from threading import Thread

producer_config = {'port': 5557, 'messagesMemoryBuff': 5000, 'encoding': 'msgpack', 'statisticsInterval': 0.1}
listenr_config = {'remoteAddress': 'tcp://localhost:5557', 'encoding': 'msgpack', 'messageOriginNodeName': 'b'}


def test_Messaging():
    asserts = {}
    asserts['responses'] = 0
    messageProducer = MessageProducer(producer_config, ['a'])

    def onStatistics(statistics):
        asserts['stats'] = statistics
        asserts['responses'] = int(statistics[0]['responses'])

    messageProducer.registerStatisticsListener(onStatistics)
    time.sleep(3)

    def onMessage(msg, origin):
        # pylint: disable=unused-argument
        if (type(msg) == type(dict())):
            asserts['field1'] = msg['field1']
        time.sleep(1)

    messageProducer.start()

    messageProducer.produce({'field1': 'value1'})
    messageProducer.produce({'field1': 'value1'})
    messageProducer.produce({'field1': 'value1'})
    time.sleep(0.5)
    assert asserts['stats'][0]['queueSize'] == 3
    assert asserts['stats'][0]['sent'] == 0
    messageListener = MessageListener(listenr_config, receiverNode='a')
    messageListener.registerMessageListener(onMessage)
    messageListener.start()
    time.sleep(4.2)
    assert asserts['field1'] == 'value1'
    assert asserts['stats'][0]['queueSize'] == 0
    assert asserts['stats'][0]['sent'] == 3
    assert asserts['responses'] == 3
    print('done\n')

    def getHello(msg, origin):
        if (msg == b'Hello'):
            asserts['gotHello'] = True

    messageListener.registerMessageListener(getHello)
    messageProducer.produce(b'Hello')
    time.sleep(3)
    assert asserts['gotHello']

    messageProducer.close()
    messageListener.close()

    time.sleep(2)
