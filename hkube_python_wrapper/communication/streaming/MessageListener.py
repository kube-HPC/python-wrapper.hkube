from hkube_python_wrapper.communication.zmq.streaming.ZMQListener import ZMQListener
import time

from hkube_python_wrapper.util.encoding import Encoding


class MessageListener(object):

    def __init__(self, options, receiverNode):
        remoteAddress = options['remoteAddress']
        self.adapater = ZMQListener(remoteAddress, self.onMessage, receiverNode)
        self.messageOriginNodeName = options['messageOriginNodeName']
        encodingType = options['encoding']
        self._encoding = Encoding(encodingType)
        self.messageListeners = []

    def registerMessageListener(self, listener):
        self.messageListeners.append(listener)

    def onMessage(self, msg):
        start = time.time()
        decodedMsg = self._encoding.decode(value=msg, plainEncode=True)
        for listener in self.messageListeners:
            try:
                listener(decodedMsg, self.messageOriginNodeName)
            except Exception as e:
                print('Error during MessageListener onMessage' + e)

        end = time.time()
        duration = int((end - start)*1000)
        return self._encoding.encode({'duration': duration}, plainEncode=True)

    def start(self):
        print("start receiving from " + self.messageOriginNodeName)
        self.adapater.start()

    def close(self):
        self.messageListeners = []
        self.adapater.close()
