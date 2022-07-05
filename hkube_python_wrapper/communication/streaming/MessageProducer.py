import time
from threading import Thread
from hkube_python_wrapper.util.encoding import Encoding
from hkube_python_wrapper.communication.zmq.streaming.producer.ZMQProducer import ZMQProducer
from hkube_python_wrapper.util.fifo_array import FifoArray
from hkube_python_wrapper.util.DaemonThread import DaemonThread
from hkube_python_wrapper.util.logger import log

RESPONSE_CACHE = 10


class MessageProducer(DaemonThread):
    def __init__(self, options, consumerNodes, nodeName):
        self.nodeNames = consumerNodes
        port = options['port']
        maxMemorySize = options['messagesMemoryBuff'] * 1024 * 1024
        encodingType = options['encoding']
        statisticsInterval = options['statisticsInterval']
        self._encoding = Encoding(encodingType)
        self.adapter = ZMQProducer(port, maxMemorySize, self.responseAccumulator, self.queueTimeAccumulator, consumerTypes=self.nodeNames, encoding=self._encoding, nodeName=nodeName)
        self.durationsCache = {}
        self.grossDurationCache = {}
        self.queueTimeCache = {}
        self.responseCount = {}
        self.active = True
        self.printStatistics = 0
        for consumer in consumerNodes:
            self.durationsCache[consumer] = FifoArray(RESPONSE_CACHE)
            self.grossDurationCache[consumer] = FifoArray(RESPONSE_CACHE)
            self.queueTimeCache[consumer] = FifoArray(RESPONSE_CACHE)
            self.responseCount[consumer] = 0
        self.listeners = []

        def sendStatisticsEvery(interval):
            while (self.active):
                self.sendStatistics()
                time.sleep(interval)
            self.sendStatistics()

        if (self.nodeNames):
            runThread = Thread(name="Statistics", target=sendStatisticsEvery, args=[statisticsInterval])
            runThread.daemon = True
            runThread.start()
        DaemonThread.__init__(self, "MessageProducer")

    def resetQueue(self):
        self.adapter.resetQueue()

    def resetQueuePartial(self, numberOfMessagesToRemove):
        self.adapter.resetQueuePartial(numberOfMessagesToRemove)

    def produce(self, messageFlowPattern, obj):
        header, encodedMessage = self._encoding.encode(obj)
        self.adapter.produce(header, encodedMessage, messageFlowPattern=messageFlowPattern)

    def responseAccumulator(self, response, consumerType, grossDuration):
        decodedResponse = self._encoding.decode(value=response, plainEncode=True)
        duration = decodedResponse['duration']
        self.durationsCache[consumerType].append(float(duration))
        self.grossDurationCache[consumerType].append(grossDuration)
        self.responseCount[consumerType] += 1

    def queueTimeAccumulator(self, consumerType, queueTime):
        self.queueTimeCache[consumerType].append(queueTime)

    def resetDurationsCache(self, consumerType):
        durationPerNode = self.durationsCache[consumerType].getAsArray()
        self.durationsCache[consumerType].reset()
        return durationPerNode

    def resetGrossDurationsCache(self, consumerType):
        durationPerNode = self.grossDurationCache[consumerType].getAsArray()
        self.grossDurationCache[consumerType].reset()
        return durationPerNode

    def resetQueueDurationsCache(self, consumerType):
        durationPerNode = self.queueTimeCache[consumerType].getAsArray()
        self.queueTimeCache[consumerType].reset()
        return durationPerNode

    def getResponseCount(self, consumerType):
        return self.responseCount[consumerType]

    def registerStatisticsListener(self, listener):
        self.listeners.append(listener)

    def getStatistics(self):
        statisticsPerNode = []
        for nodeName in self.nodeNames:
            queueSize = self.adapter.queueSize(nodeName)
            sent = self.adapter.sent(nodeName)
            singleNodeStatistics = {"nodeName": nodeName,
                                    "sent": sent,
                                    "queueSize": queueSize,
                                    "dropped": self.adapter.messageQueue.lostMessages[nodeName]}
            statisticsPerNode.append(singleNodeStatistics)
        statistics = {"binarySize": self.adapter.messageQueue.sizeSum,
                      "configuredMaxBinarySize": self.adapter.maxMemorySize,
                      "statisticsPerNode": statisticsPerNode}
        return statistics

    def sendStatistics(self):
        statistics = []
        for nodeName in self.nodeNames:
            queueSize = self.adapter.queueSize(nodeName)
            sent = self.adapter.sent(nodeName)
            singleNodeStatistics = {"nodeName": nodeName,
                                    "sent": sent,
                                    "queueSize": queueSize,
                                    "netDurations": self.resetDurationsCache(nodeName),
                                    "durations": self.resetGrossDurationsCache(nodeName),
                                    "queueDurations": self.resetQueueDurationsCache(nodeName),
                                    "responses": self.getResponseCount(nodeName),
                                    "dropped": self.adapter.messageQueue.lostMessages[nodeName]}
            statistics.append(singleNodeStatistics)
        for listener in self.listeners:
            listener(statistics)
        if (self.printStatistics % 30 == 0):
            log.debug("statistics {stats}", stats=str(statistics))
        self.printStatistics += 1

    def run(self):
        self.adapter.start()

    def close(self, force=True):
        if not (self.active):
            log.warning("Attempting to close inactive MessageProducer")
        else:
            self.adapter.close(force)
            self.active = False
