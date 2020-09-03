from __future__ import print_function, division, absolute_import
import time
import datetime
import sys
import threading
from hkube_python_wrapper.tracing import Tracer

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
        func_name = func.__name__ if PY3 else func.func_name
        print('{ttt} {func_name} start on {thread}'.format(func_name=func_name, thread=threading.current_thread().ident,ttt=time.time()))
        ret = func(*args, **kwargs)
        print('{ttt} {func_name} ended on {thread}'.format(func_name=func_name, thread=threading.current_thread().ident,ttt=time.time()))
        time2 = time.time()
        printTime(func, time1, time2)
        return ret
    return wrap


def printTimePY2(func, time1, time2):
    print('%s function took %0.3f ms' % (func.func_name, (time2 - time1) * 1000.0))


def printTimePY3(func, time1, time2):
    print('{:s} {:s} function took {:.3f} ms'.format(timeFormat(), func.__name__, (time2 - time1) * 1000.0))


def timeFormat():
    now = datetime.datetime.now()
    return now.strftime('%Y-%m-%dT%H:%M:%S.%f')


if PY3:
    printTime = printTimePY3
else:
    printTime = printTimePY2
