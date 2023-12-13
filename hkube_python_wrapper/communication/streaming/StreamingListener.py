import threading
import time
import uuid

from hkube_python_wrapper.util.DaemonThread import DaemonThread

class StreamingListener(DaemonThread):

    def __init__(self, messageListeners):
        self._listeningToMessages = True
        self._working = True
        self._messageListeners = messageListeners
        DaemonThread.__init__(self, "StreamingListener")
    def fetch(self,messageListener):
        messageListener.fetch()
    def run(self):
        while (self._listeningToMessages):
            messageListeners = self._messageListeners()
            if (not messageListeners):
                time.sleep(1)  # free some cpu
                continue
            for listener in messageListeners:
                threadName = listener.messageOriginNodeName + "_" + str(uuid.uuid4())
                listener.thread = threading.Thread(name=threadName,target=self.fetch, args=[listener])
                listener.thread.start()
            for listener in messageListeners:
                listener.thread.join()
        self._working = False

    def stop(self, force=True):
        messageListeners = self._messageListeners()
        for listener in messageListeners:
            listener.close(force)
        self._listeningToMessages = False
        while self._working:
            time.sleep(0.2)
