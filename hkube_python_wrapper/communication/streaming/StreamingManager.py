import threading

from .MessageListener import MessageListener
from .MessageProducer import MessageProducer


class StreamingManager():
    threadLocalStorage = threading.local()
    def __init__(self):
        self.messageProducer = None
        self._messageListeners = dict()
        self._inputListener = []
        self.listeningToMessages = False
        self.parsedFlows = {}
        self.defaultFlow = None

    def setParsedFlows(self, flows, defaultFlow):
        self.parsedFlows = flows
        self.defaultFlow = defaultFlow

    def setupStreamingProducer(self, onStatistics, producerConfig, nextNodes, me):
        self.messageProducer = MessageProducer(producerConfig, nextNodes, me)
        self.messageProducer.registerStatisticsListener(onStatistics)
        if (nextNodes):
            self.messageProducer.start()

    def setupStreamingListeners(self, listenerConfig, parents, nodeName):
        print("parents" + str(parents))
        for predecessor in parents:
            remoteAddress = predecessor['address']
            remoteAddressUrl = 'tcp://{host}:{port}'.format(host=remoteAddress['host'], port=remoteAddress['port'])
            if (predecessor['type'] == 'Add'):
                options = {}
                options.update(listenerConfig)
                options['remoteAddress'] = remoteAddressUrl
                options['messageOriginNodeName'] = predecessor['nodeName']
                listener = MessageListener(options, nodeName, self)
                listener.registerMessageListener(self._onMessage)
                self._messageListeners[remoteAddressUrl] = listener
                if (self.listeningToMessages):
                    listener.start()
            if (predecessor['type'] == 'Del'):
                if (self.listeningToMessages):
                    self._messageListeners[remoteAddressUrl].close()
                del self._messageListeners[remoteAddressUrl]

    def registerInputListener(self, onMessage):
        self._inputListener.append(onMessage)

    def _onMessage(self, messageFlowPattern, msg, origin):
        self.threadLocalStorage.messageFlowPattern = messageFlowPattern
        for listener in self._inputListener:
            try:
                listener(msg, origin)
            except Exception as e:
                print("hkube_api message listener through exception: " + str(e))
        self.threadLocalStorage.messageFlowPattern = []

    def startMessageListening(self):
        self.listeningToMessages = True
        for listener in self._messageListeners.values():
            if not (listener.is_alive()):
                listener.start()

    def sendMessage(self, msg, flowName=None):
        if (self.messageProducer is None):
            raise Exception('Trying to send a message from a none stream pipeline or after close had been applied on algorithm')
        if (self.messageProducer.nodeNames):
            if (flowName is None):
                if (self.defaultFlow is None):
                    raise Exception("Streaming default flow is None")
                flowName = self.defaultFlow
            parsedFlow = self.parsedFlows.get(flowName)
            if (parsedFlow is None):
                raise Exception("No such flow " + flowName)
            self.messageProducer.produce(parsedFlow, msg)

    def stopStreaming(self):
        if (self.listeningToMessages):
            for listener in self._messageListeners.values():
                listener.close()
            self._messageListeners = dict()
        self.listeningToMessages = False
        self._inputListener = []
        if (self.messageProducer is not None):
            self.messageProducer.close()
            self.messageProducer = None
