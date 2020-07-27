from hkube_python_wrapper.communication.zmq.streaming.ZMQListener import ZMQListener
import time

from hkube_python_wrapper.util.encoding import Encoding


class MessageListener(object):
    def __init__(self, options, onMessage, consumerType):
        self.onMessageDelegate = onMessage
        remoteAddress = options['remoteAddress']
        self.adapater = ZMQListener(remoteAddress, self.onMessage, consumerType)
        encodingType = options['encoding']
        self._encoding = Encoding(encodingType)

    def onMessage(self, msg):
        start = time.time()
        decodedMsg = self._encoding.decode(msg, plain_encode=True)
        self.onMessageDelegate(decodedMsg)
        end = time.time()
        return self._encoding.encode({'duration': (end - start)}, plain_encode=True)

    def start(self):
        self.adapater.start()

    def close(self):
        self.adapater.close()
