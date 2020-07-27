from hkube_python_wrapper.util.encoding import Encoding
from hkube_python_wrapper.communication.zmq.streaming.ZMQProducer import ZMQProducer

RESPONSE_CACHE = 2000


class MessageProducer(object):
    def __init__(self, options, consumerNames):
        port = options['port']
        maxMemorySize = options['messageMemoryBuff']
        encodingType = options['encoding']
        self._encoding = Encoding(encodingType)
        self.adapter = ZMQProducer(port, maxMemorySize, self.responseAccumulator, consumerNames=consumerNames)
        self.responses = []

    def produce(self, obj):
        encodedMessage = self._encoding.encode(obj, plain_encode=True)
        self.adapter.produce(encodedMessage)

    def responseAccumulator(self, response):
        print(" add response\n")
        decodedResponse = self._encoding.decode(response, plain_encode=True)
        duration = decodedResponse['duration']
        self.responses.append(float(duration))
        if (len(self.responses) > RESPONSE_CACHE):
            self.responses.pop(0)

    def getMessageProcessTime(self):
        timeSum = 0
        for proccessTime in self.responses:
            timeSum += proccessTime
        return timeSum / len(self.responses)

    def start(self):
        self.adapter.start()

    def close(self):
        self.adapter.close()
