import threading
from .MessageListener import MessageListener
from .MessageProducer import MessageProducer
from .StreamingListener import StreamingListener
from hkube_python_wrapper.util.logger import log


class StreamingManager():

    def __init__(self):
        self.threadLocalStorage = threading.local()
        self.threadLocalStorage.sendMessageId = None
        self.messageProducer = None
        self._messageListeners = dict()
        self._inputListener = []
        self.listeningToMessages = False
        self._isStarted = False
        self.parsedFlows = {}
        self.defaultFlow = None
        self._streamingListener = None


    def setParsedFlows(self, flows, defaultFlow):
        self.parsedFlows = flows
        self.defaultFlow = defaultFlow

    def setupStreamingProducer(self, onStatistics, producerConfig, nextNodes, nodeName):
        self.messageProducer = MessageProducer(producerConfig, nextNodes, nodeName)
        self.messageProducer.registerStatisticsListener(onStatistics)
        if (nextNodes):
            self.messageProducer.start()

    def setupStreamingListeners(self, listenerConfig, parents, nodeName):
        log.debug("parents {parents}", parents=str(parents))
        for parent in parents:
            parentName = parent['nodeName']
            remoteAddress = parent['address']
            remoteAddressUrl = 'tcp://{host}:{port}'.format(host=remoteAddress['host'], port=remoteAddress['port'])

            if (parent['type'] == 'Add'):
                options = {}
                options.update(listenerConfig)
                options['remoteAddress'] = remoteAddressUrl
                options['messageOriginNodeName'] = parentName
                listener = MessageListener(options, nodeName)
                listener.registerMessageListener(self.onMessage)
                self._messageListeners[remoteAddressUrl] = listener

            if (parent['type'] == 'Del'):
                listener = self._messageListeners.get(remoteAddressUrl)
                if (listener):
                    closed = listener.close(force=False)
                    if (closed):
                        self._messageListeners.pop(remoteAddressUrl, None)

    def registerInputListener(self, onMessage):
        self._inputListener.append(onMessage)

    def onMessage(self, messageFlowPattern, msg, origin, sendMessageId=None):
        self.threadLocalStorage.sendMessageId = sendMessageId
        self.threadLocalStorage.messageFlowPattern = messageFlowPattern
        if (not self._inputListener):
            log.error('no input listeners on _onMessage method')
            return
        for listener in self._inputListener:
            try:
                listener(msg, origin)
            except Exception as e:
                log.error("hkube_api message listener threw exception: {e}", e=str(e))
        self.threadLocalStorage.messageFlowPattern = []

    def _getMessageListeners(self):
        return list(self._messageListeners.values())

    def startMessageListening(self):
        self.listeningToMessages = True
        if (self._isStarted is False):
            self._isStarted = True
            self._streamingListener = StreamingListener(self._getMessageListeners)
            self._streamingListener.start()

    def sendMessage(self, msg, flowName=None):
        if (self.messageProducer is None):
            raise Exception('Trying to send a message from a none stream pipeline or after close had been applied on algorithm')
        if (self.messageProducer.nodeNames):
            parsedFlow = None
            if (flowName is None):
                if hasattr(self.threadLocalStorage, 'messageFlowPattern') and self.threadLocalStorage.messageFlowPattern:
                    parsedFlow = self.threadLocalStorage.messageFlowPattern
                else:
                    if (self.defaultFlow is None):
                        raise Exception("Streaming default flow is None")
                    flowName = self.defaultFlow
            if not (parsedFlow):
                parsedFlow = self.parsedFlows.get(flowName)
            if (parsedFlow is None):
                raise Exception("No such flow " + flowName)
            self.messageProducer.produce(parsedFlow, msg)
        else:
            log.error("messageProducer has no consumers")

    def stopStreaming(self, force=True):
        if (self.listeningToMessages):
            self._isStarted = False
            self.listeningToMessages = False
            if (self._streamingListener):
                self._streamingListener.stop(force)
            self._messageListeners = dict()
            self._inputListener = []

        if (self.messageProducer is not None):
            self.messageProducer.close(force)
            self.messageProducer = None

    def clearMessageListeners(self):
        self._messageListeners = dict()
