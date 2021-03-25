from hkube_python_wrapper.communication.streaming.StreamingManager import StreamingManager
from hkube_python_wrapper.communication.streaming.MessageListener import MessageListener
from hkube_python_wrapper.communication.streaming.MessageProducer import MessageProducer
import time

parsedFlows = {
    'analyze': [{'source': 'A', 'next': ['B']}, {'source': 'B', 'next': ['C']}, {'source': 'C', 'next': ['D']}], 
    'master': [{'source': 'B', 'next': ['A', 'C']}, {'source': 'C', 'next': ['D']}, {'source': 'D', 'next': ['E']}]
}

def test_streaming_flow():
    messages = []

    def onMessage(msg, origin):
        messages.append(msg)

    def statsInvoked(args):
        print('stats')

    port = 9340
    nodeName = 'B'
    parents = [{'nodeName': 'A', 'address': {'host': '127.0.0.1', 'port': port}, 'type': 'Add'}]
    producer_config = {'port': port, 'messagesMemoryBuff': 5000, 'encoding': 'msgpack', 'statisticsInterval': 1}
    listen_config = {'encoding': 'msgpack'}
    streamingManager = StreamingManager()
    streamingManager.setParsedFlows(parsedFlows, 'analyze')
    streamingManager.setupStreamingProducer(statsInvoked, producer_config, [nodeName], 'A')
    streamingManager.setupStreamingListeners(listen_config, parents, nodeName)
    streamingManager.registerInputListener(onMessage)
    streamingManager.startMessageListening()

    streamingManager.sendMessage({'msg': '1'})
    streamingManager.sendMessage({'msg': '2'})
    streamingManager.sendMessage({'msg': '3'})
    streamingManager.sendMessage({'msg': '4'})

    time.sleep(1)

    assert len(messages) == 4
    assert messages[0] == {'msg': '1'}
    assert messages[1] == {'msg': '2'}
    assert messages[2] == {'msg': '3'}
    assert messages[3] == {'msg': '4'}
    streamingManager.stopStreaming()

def test_streaming_manager():
    resultsAtB = {}
    resultsAtC = {}

    def onMessageAtC(msg, origin):
        resultsAtC['data'] = msg
        resultsAtC['origin'] = origin

    def onMessageAtB(msg, origin):
        resultsAtB['data'] = msg
        resultsAtB['origin'] = origin
        streamingManagerB.sendMessage('msg from B')

    def statsInvoked(args):
        print('stats')

    port1 = 9341
    port2 = 9342
    parents1 = [{'nodeName': 'A', 'address': {'host': '127.0.0.1', 'port': port1}, 'type': 'Add'}]
    parents2 = [{'nodeName': 'B', 'address': {'host': '127.0.0.1', 'port': port2}, 'type': 'Add'}]
    producer_configA = {'port': port1, 'messagesMemoryBuff': 5000, 'encoding': 'msgpack', 'statisticsInterval': 1}
    producer_configB = {'port': port2, 'messagesMemoryBuff': 5000, 'encoding': 'msgpack', 'statisticsInterval': 1}
    listen_config = {'encoding': 'msgpack'}

    streamingManagerA = StreamingManager()
    streamingManagerA.setParsedFlows(parsedFlows, 'analyze')
    streamingManagerA.setupStreamingProducer(statsInvoked, producer_configA, ['B'], 'A')

    streamingManagerB = StreamingManager()
    streamingManagerB.setParsedFlows(parsedFlows, 'analyze')
    streamingManagerB.setupStreamingProducer(statsInvoked, producer_configB, ['C'], 'B')
    streamingManagerB.setupStreamingListeners(listen_config, parents1, 'B')
    streamingManagerB.registerInputListener(onMessageAtB)
    streamingManagerB.startMessageListening()

    streamingManagerC = StreamingManager()
    streamingManagerC.setupStreamingListeners(listen_config, parents2, 'C')
    streamingManagerC.registerInputListener(onMessageAtC)
    streamingManagerC.startMessageListening()

    streamingManagerA.sendMessage('msg from A')
    time.sleep(1)
    assert resultsAtB['data'] == 'msg from A'
    assert resultsAtB['origin'] == 'A'
    assert resultsAtC['data'] == 'msg from B'
    assert resultsAtC['origin'] == 'B'
    streamingManagerA.stopStreaming()
    streamingManagerB.stopStreaming()

def xtest_messaging():

    producer_config = {'port': 9536, 'messagesMemoryBuff': 5000, 'encoding': 'msgpack', 'statisticsInterval': 1}
    listenr_config = {'remoteAddress': 'tcp://localhost:9536', 'encoding': 'msgpack', 'messageOriginNodeName': 'b'}
    asserts = {}
    asserts['responses'] = 0
    messageProducer = MessageProducer(producer_config, ['a'], 'b')

    def onStatistics(statistics):
        asserts['stats'] = statistics
        asserts['responses'] = int(statistics[0]['responses'])

    messageProducer.registerStatisticsListener(onStatistics)
    time.sleep(3)

    def onMessage(envelope, msg, origin):
        if (type(msg) == type(dict())):
            asserts['field1'] = msg['field1']
        time.sleep(0.1)

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
    time.sleep(2)
    assert asserts['stats'][0]['queueSize'] == 3
    assert asserts['stats'][0]['sent'] == 0
    messageListener = MessageListener(listenr_config, receiverNode='a')
    messageListener.registerMessageListener(onMessage)
    for _ in range(1,5):
        if (asserts.get('field1')):
            break
        time.sleep(1)
    time.sleep(4)
    assert asserts['field1'] == 'value1'
    assert asserts['stats'][0]['queueSize'] == 0
    assert asserts['stats'][0]['sent'] == 3
    assert asserts['responses'] == 3

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

def xtest_messaging_split():
    producer_config = {'port': 9536, 'messagesMemoryBuff': 5000, 'encoding': 'msgpack', 'statisticsInterval': 1}
    listenr_config_c = {'remoteAddress': 'tcp://localhost:9536', 'encoding': 'msgpack', 'messageOriginNodeName': 'c'}
    asserts = {}
    asserts['responses'] = 0
    messageProducer = MessageProducer(producer_config, ['a','b'], 'c')

    def onStatistics(statistics):
        asserts['stats'] = statistics
        asserts['responses'] = int(statistics[0]['responses'])

    messageProducer.registerStatisticsListener(onStatistics)
    time.sleep(3)

    def onMessage(envelope, msg, origin):
        # pylint: disable=unused-argument
        if (type(msg) == type(dict())):
            asserts['field1'] = msg['field1']
        time.sleep(0.1)

    messageProducer.start()
    env = [{
        "source": "c",
        "next": [
            "a"
        ]
    }]
    env2 = [{
        "source": "c",
        "next": [
            "b"
        ]
    }]
    messageProducer.produce(env, {'field1': 'value1'})
    messageProducer.produce(env2, {'field1': 'value2'})
    messageProducer.produce(env, {'field1': 'value1'})
    messageProducer.produce(env2, {'field1': 'value2'})
    messageProducer.produce(env2, {'field1': 'value2'})
    messageProducer.produce(env, {'field1': 'value1'})
    time.sleep(2)
    assert asserts['stats'][0]['queueSize'] == 3
    assert asserts['stats'][0]['sent'] == 0

    messageListener = MessageListener(listenr_config_c, receiverNode='a')
    messageListener.registerMessageListener(onMessage)
    for _ in range(1,5):
        if ( asserts.get('field1')):
            break
        time.sleep(1)
    time.sleep(2)
    assert asserts['field1'] == 'value1'
    assert asserts['stats'][0]['queueSize'] == 0
    assert asserts['stats'][0]['sent'] == 3
    assert asserts['stats'][1]['queueSize'] == 3
    assert asserts['stats'][1]['sent'] == 0
    assert asserts['responses'] == 3
    messageListener2 = MessageListener(listenr_config_c, receiverNode='b')
    messageListener2.registerMessageListener(onMessage)
    messageListener2.start()
    time.sleep(20)
    assert asserts['stats'][1]['queueSize'] == 0
    assert asserts['stats'][1]['sent'] == 3
    assert len(messageProducer.adapter.messageQueue.queue) == 0
