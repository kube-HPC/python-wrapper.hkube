import multiprocessing
import threading

from .MessageListener import MessageListener
from hkube_python_wrapper.util.logger import log, algorithmLogger
from .producerProcess import ProducerRunner


class StreamingManager():

    def __init__(self):
        self.threadLocalStorage = threading.local()
        self.threadLocalStorage.sendMessageId = None
        self._messageListeners = dict()
        self._inputListener = []
        self.listeningToMessages = False
        self._isStarted = False
        self.parsedFlows = {}
        self.defaultFlow = None
        self.processQueue = None
        self.nextNodes = []

    def setParsedFlows(self, flows, defaultFlow):
        self.parsedFlows = flows
        self.defaultFlow = defaultFlow

    def setupStreamingProducer(self, options, producerConfig, nextNodes, nodeName):
        self.nextNodes = nextNodes

        def method_in_new_process(queue, options, nextNodes, node):
            producerRunner = ProducerRunner(queue, options, nextNodes, producerConfig, node)
            producerRunner.run()

        self.processQueue = queue = multiprocessing.Queue()  # Create a queue

        p = multiprocessing.Process(target=method_in_new_process, args=(queue, options, nextNodes, nodeName))
        p.start()

        # self.messageProducer = MessageProducer(producerConfig, nextNodes, nodeName)
        # self.messageProducer.registerStatisticsListener(onStatistics)
        # if (nextNodes):
        #     self.messageProducer.start()

    # def resetQueue(self):
    #     self.messageProducer.resetQueue()
    #
    # def resetQueuePartial(self, numberOfMessagesToRemove):
    #     self.messageProducer.resetQueuePartial(numberOfMessagesToRemove)

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

                def is_active():
                    return self._messageListeners.get(
                        remoteAddressUrl) is not None and self.listeningToMessages  # pylint: disable=cell-var-from-loop

                listener = MessageListener(options, nodeName, is_active)
                listener.registerMessageListener(self.onMessage)
                self._messageListeners[remoteAddressUrl] = listener
                if (self.listeningToMessages):
                    listener.start()

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
                algorithmLogger.exception(e)
        self.threadLocalStorage.messageFlowPattern = []

    def _getMessageListeners(self):
        return self._messageListeners

    def startMessageListening(self):
        self.listeningToMessages = True
        if (self._isStarted is False):
            self._isStarted = True
            messageListeners = list(self._messageListeners.values())
            for listener in messageListeners:
                listener.start()

    def sendMessage(self, msg, flowName=None):
        if (self.processQueue is None):
            raise Exception(
                'Trying to send a message from a none stream pipeline or after close had been applied on algorithm')
        if (self.nextNodes):
            parsedFlow = None
            if (flowName is None):
                if hasattr(self.threadLocalStorage,
                           'messageFlowPattern') and self.threadLocalStorage.messageFlowPattern:
                    parsedFlow = self.threadLocalStorage.messageFlowPattern
                else:
                    if (self.defaultFlow is None):
                        raise Exception("Streaming default flow is None")
                    flowName = self.defaultFlow
            if not (parsedFlow):
                parsedFlow = self.parsedFlows.get(flowName)
            if (parsedFlow is None):
                raise Exception("No such flow " + flowName)
            self.processQueue.put({"flow": parsedFlow, "msg": msg})
        else:
            log.error("messageProducer has no consumers")

    def stopStreaming(self, force=True):
        if (self.listeningToMessages):
            self._isStarted = False
            self.listeningToMessages = False
            messageListeners = list(self._messageListeners.values())
            for listener in messageListeners:
                listener.close(force)
            for listener in messageListeners:
                listener.join()
            self._messageListeners = dict()
            self._inputListener.clear()

        if (self.processQueue is not None):
            self.processQueue.put({"action": "stop", "force": True})
            print("sent stop")
            done = self.processQueue.get();
            print("got " + str(done) + " stop from producer process")

    def clearMessageListeners(self):
        self._messageListeners = dict()
