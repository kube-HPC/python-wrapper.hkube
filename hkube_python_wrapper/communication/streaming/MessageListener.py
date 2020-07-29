from hkube_python_wrapper.communication.zmq.streaming.ZMQListener import ZMQListener
import time

from hkube_python_wrapper.util.encoding import Encoding


class MessageListener(object):

    def __init__(self, options, consumerType):
        remoteAddress = options['remoteAddress']
        self.adapater = ZMQListener(remoteAddress, self.onMessage, consumerType)
        encodingType = options['encoding']
        self._encoding = Encoding(encodingType)
        self.messageListeners = []

    def registerMessageListener(self, listener):
        self.messageListeners.append(listener)

    def onMessage(self, msg):
        start = time.time()
        decodedMsg = self._encoding.decode(msg, plain_encode=True)
        for listener in self.messageListeners:
            listener(decodedMsg)
        end = time.time()
        return self._encoding.encode({'duration': (end - start)}, plain_encode=True)

    def start(self):
        self.adapater.start()

    def close(self):
        self.adapater.close()
