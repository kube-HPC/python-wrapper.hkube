from __future__ import print_function, division, absolute_import
import time
import datetime
import sys
from hkube_python_wrapper.tracing import Tracer
from hkube_python_wrapper.util.logger import log

PY3 = sys.version_info[0] == 3


def trace(name=None):
    def func_wrap(func):
        def wrap(*args, **kwargs):
            if (Tracer.instance is None):
                return func(*args, **kwargs)
            func_name = func.__name__ if PY3 else func.func_name
            operation_name = func_name if name is None else name
            with Tracer.instance.tracer.start_active_span(operation_name=operation_name):
                return func(*args, **kwargs)
        return wrap
    return func_wrap


def timing(func):
    def wrap(*args, **kwargs):
        time1 = time.time()
        ret = func(*args, **kwargs)
        time2 = time.time()
        printTime(func, time1, time2)
        return ret
    return wrap


def printTimePY2(func, time1, time2):
    log.debug('{name} function took {time:0.3} ms', name=func.func_name, time=(time2 - time1) * 1000.0)


def printTimePY3(func, time1, time2):
    log.debug('{time} {name} function took {diff:.3f} ms', time=timeFormat(), name=func.__name__, diff=(time2 - time1) * 1000.0)


def timeFormat():
    now = datetime.datetime.now()
    return now.strftime('%Y-%m-%dT%H:%M:%S')


if PY3:
    printTime = printTimePY3
else:
    printTime = printTimePY2
