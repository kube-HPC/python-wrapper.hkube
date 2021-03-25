import time
from hkube_python_wrapper.util.DaemonThread import DaemonThread

class StreamingListener(DaemonThread):

    def __init__(self, messageListeners):
        self._counter = 0
        self._timer = None
        self._listeningToMessages = True
        self._messageListeners = messageListeners
        DaemonThread.__init__(self, "StreamingListener")

    def run(self):
        while(self._listeningToMessages):
            if(self._timer is None):
                self._timer = time.time()
            
            if(time.time() - self._timer > 1):
                print('total counter {counter}'.format(counter=self._counter))
                self._counter = 0
                self._timer = None
            
            messageListeners = self._messageListeners()
            for listener in list(messageListeners.values()):
                listener.fetch()
                self._counter += 1
            time.sleep(0.008) # free some cpu

    def stop(self, force=True):
        messageListeners = self._messageListeners()
        for listener in list(messageListeners.values()):
            listener.close(force)
        self._listeningToMessages = False
