from hkube_python_wrapper.communication.zmq.streaming.ZMQListener import ZMQListener
from hkube_python_wrapper.util.encoding import Encoding
from hkube_python_wrapper.util.DaemonThread import DaemonThread
from hkube_python_wrapper.util.logger import log
import time

class MessageListener(DaemonThread):

    def __init__(self, options, receiverNode, errorHandler=None, onReady=None, onNotReady=None):
        self.errorHandler = errorHandler
        remoteAddress = options['remoteAddress']
        encodingType = options['encoding']
        self._encoding = Encoding(encodingType)
        self.adapater = ZMQListener(remoteAddress, self.onMessage, self._encoding, receiverNode, onReady, onNotReady)
        self.messageOriginNodeName = options['messageOriginNodeName']
        self.messageListeners = []
        DaemonThread.__init__(self, "MessageListener-" + str(self.messageOriginNodeName))

    def registerMessageListener(self, listener):
        self.messageListeners.append(listener)

    def ready(self):
        self.adapater.ready()

    def notReady(self):
        self.adapater.notReady()

    def onMessage(self, messageFlowPattern, header, msg):
        start = time.time()
        decodedMsg = self._encoding.decode(header=header, value=msg)
        for listener in self.messageListeners:
            try:
                listener(messageFlowPattern, decodedMsg, self.messageOriginNodeName)
            except Exception as e:
                log.error('Error during MessageListener onMessage {e}', e=str(e))

        end = time.time()
        duration = float((end - start) * 1000)
        return self._encoding.encode({'duration': round(duration, 4)}, plainEncode=True)

    def run(self):
        log.info("Start receiving from {node}", node=self.messageOriginNodeName)
        try:
            self.adapater.start()
        except Exception as e:
            if (self.errorHandler):
                self.errorHandler(e)

    def close(self, force=True):
        self.adapater.close(force)
        self.messageListeners = []
