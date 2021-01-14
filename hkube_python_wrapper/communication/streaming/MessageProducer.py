import time
from threading import Thread

from hkube_python_wrapper.util.encoding import Encoding
from hkube_python_wrapper.communication.zmq.streaming.producer.ZMQProducer import ZMQProducer
from hkube_python_wrapper.util.fifo_array import FifoArray
from hkube_python_wrapper.util.DaemonThread import DaemonThread

RESPONSE_CACHE = 2000


class MessageProducer(DaemonThread):
    def __init__(self, options, consumerNodes, me):
        self.nodeNames = consumerNodes
        port = options['port']
        maxMemorySize = options['messagesMemoryBuff'] * 1024 * 1024
        encodingType = options['encoding']
        statisticsInterval = options['statisticsInterval']
        self._encoding = Encoding(encodingType)
        self.adapter = ZMQProducer(port, maxMemorySize, self.responseAccumulator, consumerTypes=self.nodeNames, encoding=self._encoding, me=me)
        self.responsesCache = {}
        self.responseCount = {}
        self.active = True
        self.printStatistics = 0
        for nodeName in consumerNodes:
            self.responsesCache[nodeName] = FifoArray(RESPONSE_CACHE)
            self.responseCount[nodeName] = 0
        self.listeners = []

        def sendStatisticsEvery(interval):
            while (self.active):
                self.sendStatistics()
                time.sleep(interval)

        if (self.nodeNames):
            runThread = Thread(name="Statistics", target=sendStatisticsEvery, args=[statisticsInterval])
            runThread.daemon = True
            runThread.start()
        DaemonThread.__init__(self, "MessageProducer")

    def produce(self, meesageFlowPattern, obj):
        header, encodedMessage = self._encoding.encode(obj)
        self.adapter.produce(header, encodedMessage, messageFlowPattern=meesageFlowPattern)

    def responseAccumulator(self, response, consumerType):
        decodedResponse = self._encoding.decode(value=response, plainEncode=True)
        self.responseCount[consumerType] += 1
        duration = decodedResponse['duration']
        self.responsesCache[consumerType].append(float(duration))

    def resetResponseCache(self, consumerType):
        responsePerNode = self.responsesCache[consumerType].getAsArray()
        self.responsesCache[consumerType].reset()
        return responsePerNode

    def getResponseCount(self, consumerType):
        return self.responseCount[consumerType]

    def registerStatisticsListener(self, listener):
        self.listeners.append(listener)

    def sendStatistics(self):
        statistics = []
        for nodeName in self.nodeNames:
            queueSize = self.adapter.queueSize(nodeName)
            sent = self.adapter.sent(nodeName)
            singleNodeStatistics = {"nodeName": nodeName, "sent": sent, "queueSize": queueSize,
                                    "durations": self.resetResponseCache(nodeName),
                                    "responses": self.getResponseCount(nodeName),
                                    "dropped": self.adapter.messageQueue.lostMessages[nodeName]}
            statistics.append(singleNodeStatistics)
        for listener in self.listeners:
            listener(statistics)
        for singleNodeStatisticsForPrint in statistics:
            singleNodeStatisticsForPrint['durations'] = singleNodeStatisticsForPrint['durations'][:10]
        if (self.printStatistics % 10 == 0):
            print("statistics " + str(statistics))
        self.printStatistics += 1

    def run(self):
        self.adapter.start()

    def close(self, force=True):
        if not (self.active):
            print("Attempting to close inactive MessageProducer")
        else:
            self.active = False
            self.adapter.close(force)
