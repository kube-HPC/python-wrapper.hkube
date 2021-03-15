from hkube_python_wrapper.communication.zmq.streaming.ZMQListener import ZMQListener
from hkube_python_wrapper.util.encoding import Encoding
from hkube_python_wrapper.util.DaemonThread import DaemonThread
from hkube_python_wrapper.util.logger import log
import time

class MessageListener(DaemonThread):

    def __init__(self, options, consumerType, errorHandler=None, onReady=None, onNotReady=None):
        self.errorHandler = errorHandler
        remoteAddress = options['remoteAddress']
        encodingType = options['encoding']
        self.address = remoteAddress
        self._encoding = Encoding(encodingType)
        self.messageOriginNodeName = options['messageOriginNodeName']
        self.adapater = ZMQListener(remoteAddress, consumerType, self.messageOriginNodeName, self.onMessage, self._encoding, onReady, onNotReady)
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
        try:
            self.adapater.start()
        except Exception as e:
            if (self.errorHandler):
                self.errorHandler(e)

    def close(self, force=True):
        try:
            self.adapater.close(force)
        except Exception as e:
            log.error('Exception in adapater.close {e}', e=str(e))

    def waitForClose(self):
        self.adapater.waitForClose()
        self.messageListeners = []
