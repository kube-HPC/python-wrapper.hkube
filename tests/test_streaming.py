from hkube_python_wrapper.communication.streaming.StreamingManager import StreamingManager
from hkube_python_wrapper.communication.streaming.MessageListener import MessageListener
from hkube_python_wrapper.communication.streaming.MessageProducer import MessageProducer
import time

producer_config = {'port': 9126, 'messagesMemoryBuff': 5000, 'encoding': 'msgpack', 'statisticsInterval': 1}
listenr_config = {'remoteAddress': 'tcp://localhost:9126', 'encoding': 'msgpack', 'messageOriginNodeName': 'b'}
parsedFlows = {'analyze': [{'source': 'A', 'next': ['B']}, {'source': 'B', 'next': ['C']}, {'source': 'C', 'next': ['D']}], 'master': [{'source': 'A', 'next': ['B', 'C']}, {'source': 'C', 'next': ['D']}]}
parents = [{'nodeName': 'A', 'address': {'host': '127.0.0.1', 'port': '9126'}, 'type': 'Add'}]


def test_streaming_manager():
    parents = [{'nodeName': 'A', 'address': {'host': '127.0.0.1', 'port': '9126'}, 'type': 'Add'}]
    streamingManagaer = StreamingManager()
    streamingManagaer.setParsedFlows(parsedFlows, 'analyze')
    results = {}
    def onMessage(flow,msg, origin):
        results['flowLength'] = len(flow)
        results['flowFirstSource'] = flow[0]['source']

    def statsInvoked():
        print('stats')
    streamingManagaer.setupStreamingProducer(statsInvoked , producer_config, ['B'], 'A')

    messageListener = MessageListener(listenr_config, receiverNode='B')
    messageListener.registerMessageListener(onMessage)
    messageListener.start()
    streamingManagaer.sendMessage('klum')
    time.sleep(1)

    assert results['flowLength'] == 2
    assert results['flowFirstSource'] == 'B'
    streamingManagaer.sendMessage('klum','master')
    time.sleep(1)
    assert results['flowLength'] == 1
    assert results['flowFirstSource'] == 'C'
    streamingManagaer.stopStreaming()


def test_Messaging():
    asserts = {}
    asserts['responses'] = 0
    messageProducer = MessageProducer(producer_config, ['a'], 'b')

    def onStatistics(statistics):
        asserts['stats'] = statistics
        asserts['responses'] = int(statistics[0]['responses'])

    messageProducer.registerStatisticsListener(onStatistics)
    time.sleep(3)

    def onMessage(envelope, msg, origin):
        # pylint: disable=unused-argument
        if (type(msg) == type(dict())):
            asserts['field1'] = msg['field1']
        time.sleep(1)

    messageProducer.start()
    env = [{
        "source": "b",
        "next": [
            "a"
        ]
    }, {
        "source": "a",
        "next": [
            "c"
        ]
    }]
    messageProducer.produce(env, {'field1': 'value1'})
    messageProducer.produce(env, {'field1': 'value1'})
    messageProducer.produce(env, {'field1': 'value1'})
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

    def getHello(envelope, msg, origin):
        if (msg == b'Hello'):
            asserts['gotHello'] = True
            asserts['envelope'] = envelope

    messageListener.registerMessageListener(getHello)
    messageProducer.produce(env, b'Hello')
    time.sleep(3)
    assert asserts['gotHello']
    assert len(asserts['envelope']) == 1
    assert asserts['envelope'][0]['source'] == 'a'

    messageProducer.close()
    messageListener.close()

    time.sleep(2)
