import threading
import sys

if (sys.version_info > (3, 0)):
    # Python 3 code in this block
    # pylint: disable=inherit-non-class
    class Timer(threading.Timer):
        def __init__(self, interval, function, args=None, kwargs=None, name='', daemon=True):
            # pylint: disable=non-parent-init-called
            threading.Timer.__init__(self, interval, function, args, kwargs)
            self.name = name
            self.daemon = daemon
else:
    # Python 2 code in this block
    class Timer():
        def __init__(self, interval, function, args=[], kwargs={}, name='', daemon=True):
            self.interval = interval
            self.function = function
            self.args = args
            self.kwargs = kwargs
            self.name = name
            self.daemon = daemon
            self._timer = None

        def start(self):
            self._timer = threading.Timer(self.interval, self.function, self.args, self.kwargs)
            self._timer.name = self.name
            self._timer.daemon = self.daemon
            self._timer.start()
