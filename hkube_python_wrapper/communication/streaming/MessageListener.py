from hkube_python_wrapper.communication.zmq.streaming.ZMQListener import ZMQListener
from hkube_python_wrapper.util.encoding import Encoding
from hkube_python_wrapper.util.logger import log
import time
from hkube_python_wrapper.util.DaemonThread import DaemonThread

class MessageListener(DaemonThread):

    def __init__(self, options, receiverNode,isActiveFunc):
        self.isActiveFunc = isActiveFunc
        remoteAddress = options['remoteAddress']
        encodingType = options['encoding']
        self._encoding = Encoding(encodingType)
        delay = options['delay']
        self.adapater = ZMQListener(remoteAddress, self.onMessage, self._encoding, receiverNode, delay)
        self.messageOriginNodeName = options['messageOriginNodeName']
        self.messageListeners = []
        DaemonThread.__init__(self, remoteAddress)

    def registerMessageListener(self, listener):
        self.messageListeners.append(listener)

    def onMessage(self, messageFlowPattern, header, msg):
        start = time.time()
        decodedMsg = self._encoding.decode(header=header, value=msg)
        for listener in self.messageListeners:
            try:
                listener(messageFlowPattern, decodedMsg, self.messageOriginNodeName)
            except Exception as e:
                log.error('Error during MessageListener onMessage {e}', e=str(e))
                log.exception(e)

        end = time.time()
        duration = float((end - start) * 1000)
        return self._encoding.encode({'duration': round(duration, 4)}, plainEncode=True)

    def fetch(self):
        self.adapater.fetch()
    def run(self):
        while(self.isActiveFunc()):
            self.adapater.fetch()

    def close(self, force=True):
        closed = False
        try:
            closed = self.adapater.close(force)
        except Exception as e:
            log.error('Exception in adapater.close {e}', e=str(e))
        return closed
