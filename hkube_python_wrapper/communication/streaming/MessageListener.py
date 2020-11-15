from hkube_python_wrapper.communication.zmq.streaming.ZMQListener import ZMQListener
import time

from hkube_python_wrapper.util.encoding import Encoding

#
from hkube_python_wrapper.util.DaemonThread import DaemonThread


class MessageListener(DaemonThread):

    def __init__(self, options, receiverNode, errorHandler=None):
        self.errorHandler = errorHandler
        remoteAddress = options['remoteAddress']
        self.adapater = ZMQListener(remoteAddress, self.onMessage, receiverNode)
        self.messageOriginNodeName = options['messageOriginNodeName']
        encodingType = options['encoding']
        self._encoding = Encoding(encodingType)
        self.messageListeners = []
        DaemonThread.__init__(self, "MessageListener-"+ str(self.messageOriginNodeName))

    def registerMessageListener(self, listener):
        self.messageListeners.append(listener)

    def onMessage(self, header, msg):
        start = time.time()
        decodedMsg = self._encoding.decode(header=header, value=msg)
        for listener in self.messageListeners:
            try:
                listener(decodedMsg, self.messageOriginNodeName)
            except Exception as e:
                print('Error during MessageListener onMessage' + e)

        end = time.time()
        duration = int((end - start) * 1000)
        return self._encoding.encode({'duration': duration}, plainEncode=True)

    def run(self):
        print("start receiving from " + self.messageOriginNodeName)
        try:
            self.adapater.start()
        except Exception as e:
            self.errorHandler.sendError(e)


    def close(self):
        self.messageListeners = []
        self.adapater.close()
