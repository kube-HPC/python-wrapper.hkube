import time
from hkube_python_wrapper.util.DaemonThread import DaemonThread

class StreamingListener(DaemonThread):

    def __init__(self, messageListeners):
        self._listeningToMessages = True
        self._working = True
        self._messageListeners = messageListeners
        DaemonThread.__init__(self, "StreamingListener")

    def run(self):
        while (self._listeningToMessages):
            messageListeners = self._messageListeners()
            if (not messageListeners):
                time.sleep(1)  # free some cpu
                continue
            for listener in messageListeners:
                listener.fetch()
        self._working = False

    def stop(self, force=True):
        messageListeners = self._messageListeners()
        for listener in messageListeners:
            listener.close(force)
        self._listeningToMessages = False
        while self._working:
            time.sleep(0.2)
