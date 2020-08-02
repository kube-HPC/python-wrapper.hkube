import gevent

from hkube_python_wrapper.util.encoding import Encoding
from hkube_python_wrapper.communication.zmq.streaming.ZMQProducer import ZMQProducer

RESPONSE_CACHE = 2000


class MessageProducer(object):
    def __init__(self, options, consumerNames):
        self.consumerNames = consumerNames
        port = options['port']
        maxMemorySize = options['messagesMemoryBuff']
        encodingType = options['encoding']
        statisticsInterval = options['statisticsInterval']
        self._encoding = Encoding(encodingType)
        self.adapter = ZMQProducer(port, maxMemorySize, self.responseAccumulator, consumerNames=consumerNames)
        self.responses = {}
        self.active = True
        for nodeName in consumerNames:
            self.responses[nodeName] = []
        self.listeners = []

        def sendStatisticsEvery(interval):
            while (self.active):
                self.sendStatistics()
                gevent.sleep(interval)

        gevent.spawn(sendStatisticsEvery, statisticsInterval)

    def produce(self, obj):
        encodedMessage = self._encoding.encode(obj, plain_encode=True)
        self.adapter.produce(encodedMessage)

    def responseAccumulator(self, response, consumerType):
        decodedResponse = self._encoding.decode(response, plain_encode=True)
        duration = decodedResponse['duration']
        self.responses[consumerType].append(float(duration))
        if (len(self.responses) > RESPONSE_CACHE):
            self.responses.pop(0)

    def getMessageProcessTime(self, consumerType):
        timeSum = 0
        for proccessTime in self.responses[consumerType]:
            timeSum += proccessTime
        return timeSum / len(self.responses[consumerType])

    def getResponses(self, consumerType):
        return self.responses[consumerType]

    def registerStatisticsListener(self, listener):
        self.listeners.append(listener)

    def sendStatistics(self):
        statistics = []
        for nodeName in self.consumerNames:
            queueSize = self.adapter.queueSize(nodeName)
            sent = self.adapter.sent(nodeName)
            singleNodeStatistics = {"nodeName": nodeName, "sent": sent, "queueSize": queueSize,
                                    "durationList": self.responses[nodeName]}
            statistics.append(singleNodeStatistics)
        print("statistics " + str(statistics))
        for listener in self.listeners:
            listener(statistics)

    def start(self):
        self.adapter.start()

    def close(self):
        self.active = False
        self.adapter.close()
