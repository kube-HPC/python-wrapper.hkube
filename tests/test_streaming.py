from hkube_python_wrapper.communication.streaming.StreamingManager import StreamingManager
from hkube_python_wrapper.communication.streaming.MessageListener import MessageListener
from hkube_python_wrapper.communication.streaming.MessageProducer import MessageProducer
import time

parsedFlows = {'analyze': [{'source': 'A', 'next': ['B']}, {'source': 'B', 'next': ['C']}, {'source': 'C', 'next': ['D']}], 'master': [{'source': 'B', 'next': ['A', 'C']}, {'source': 'C', 'next': ['D']},{'source': 'D', 'next': ['E']}]}
parents = [{'nodeName': 'A', 'address': {'host': '127.0.0.1', 'port': '9326'}, 'type': 'Add'}]


def test_streaming_manager():
    producer_configA = {'port': 9326, 'messagesMemoryBuff': 5000, 'encoding': 'msgpack', 'statisticsInterval': 1}
    producer_configB = {'port': 9426, 'messagesMemoryBuff': 5000, 'encoding': 'msgpack', 'statisticsInterval': 1}
    listen_toB_config = {'remoteAddress': 'tcp://localhost:9426', 'encoding': 'msgpack', 'messageOriginNodeName': 'B'}
    listen_config = {'encoding': 'msgpack'}
    parents = [{'nodeName': 'A', 'address': {'host': '127.0.0.1', 'port': '9326'}, 'type': 'Add'}]
    streamingManagaerA = StreamingManager(None)
    streamingManagaerA.setParsedFlows(parsedFlows, 'analyze')

    streamingManagaerB = StreamingManager(None)
    streamingManagaerB.setParsedFlows(parsedFlows, 'analyze')

    messageListener = MessageListener(listen_toB_config, receiverNode='C')
    resultsAtC = {}

    def onMessageB2C(flow, msg, origin):
        resultsAtC['flowLength'] = len(flow)
        resultsAtC['flowFirstSource'] = flow[0]['source']
        resultsAtC['msg'] = msg
        print("atOnmessageC")
    def onMessageAtB(msg,origin):
        print('atOnmessageB')
        streamingManagaerB.sendMessage('stam_klum')

    def statsInvoked(args):
        print('stats')

    try:
        streamingManagaerA.setupStreamingProducer(statsInvoked, producer_configA, ['B'], 'A')
        streamingManagaerB.setupStreamingProducer(statsInvoked, producer_configB, ['C'], 'B')
        streamingManagaerB.setupStreamingListeners(listen_config, parents, 'B')
        streamingManagaerB.registerInputListener(onMessageAtB)
        time.sleep(1)
        messageListener.registerMessageListener(onMessageB2C)
        streamingManagaerB.startMessageListening()

        messageListener.start()
        time.sleep(1)
        streamingManagaerA.sendMessage('klum')
        time.sleep(2)
        assert resultsAtC['flowLength'] == 1
        assert resultsAtC['flowFirstSource'] == 'C'
        assert resultsAtC['msg'] == 'stam_klum'
# send to none default flow
        streamingManagaerB.sendMessage('klum', 'master')
        time.sleep(1)
        assert resultsAtC['flowLength'] == 2
        assert resultsAtC['flowFirstSource'] == 'C'
    finally:
        streamingManagaerA.stopStreaming()
        messageListener.close()


def test_Messaging():
    try:
        producer_config = {'port': 9526, 'messagesMemoryBuff': 5000, 'encoding': 'msgpack', 'statisticsInterval': 1}
        listenr_config = {'remoteAddress': 'tcp://localhost:9526', 'encoding': 'msgpack', 'messageOriginNodeName': 'b'}
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
        time.sleep(2)
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
    finally:
        messageProducer.close()
        messageListener.close()

    time.sleep(2)
