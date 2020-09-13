from threading import Event


class WaitForDataTimeoutException(Exception):
    pass


class WaitForDataStateException(Exception):
    pass


class WaitForData(object):
    def __init__(self, autoreset=False, msg_queue=None):
        self._autoreset = autoreset
        self._msg_queue = msg_queue
        self._event = Event()
        self._event.clear()
        self._data = None

    def reset(self):
        if not self._event.is_set():
            raise WaitForDataStateException()
        self._data = None
        self._event.clear()

    def set(self, data):
        if self._event.is_set():
            raise WaitForDataStateException()
        self._data = data
        self._event.set()

    def get(self, timeout=None):
        if not self._event.wait(timeout):
            raise WaitForDataTimeoutException()
        data = self._data
        if self._autoreset:
            self.reset()
        return data

    def ready(self):
        return self._event.is_set()
