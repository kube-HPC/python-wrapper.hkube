from gevent.event import Event
from gevent.monkey import patch_all
patch_all()


class WaitForDataTimeoutException(Exception):
    pass


class WaitForDataStateException(Exception):
    pass


class WaitForData(object):
    def __init__(self, autoreset=False):
        self._autoreset = autoreset
        self._event = Event()
        self._event.clear()
        self._data = None

    def reset(self):
        if not self._event.ready():
            raise WaitForDataStateException()
        self._data = None
        self._event.clear()

    def set(self, data):
        if self._event.ready():
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
        return self._event.ready()
