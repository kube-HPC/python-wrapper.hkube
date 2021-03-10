import os
import time
import sys
from hkube_python_wrapper.util.timerImpl import Timer

if (sys.version_info > (3, 0)):
    # Python 3 code in this block
    import tracemalloc # pylint: disable=import-error
    def memoryReporting(interval=None):
        if (interval is None):
            return
        if (os.environ.get('PYTHONTRACEMALLOC', None) is None):
            return
        interval = interval / 1000

        def memoryDumpInterval():
            filename = '/tmp/dump-{name}-{timestr}'.format(name=os.environ.get('ALGORITHM_TYPE'), timestr=time.strftime("%Y%m%d-%H%M%S"))
            tracemalloc.take_snapshot().dump(filename)
            Timer(interval, memoryDumpInterval, name='memoryDumpInterval').start()

        memoryDumpInterval()
else:
    # Python 2 code in this block
    def memoryReporting(_=None):
        pass


__all__ = ['memoryReporting']
