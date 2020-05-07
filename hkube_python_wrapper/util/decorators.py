from __future__ import print_function, division, absolute_import
import time
import datetime
import sys

PY3 = sys.version_info[0] == 3


def timing(func):
    def wrap(*args, **kwargs):
        time1 = time.time()
        ret = func(*args, **kwargs)
        time2 = time.time()
        printTime(func, time1, time2)
        return ret
    return wrap


def printTimePY2(func, time1, time2):
    now = datetime.datetime.now()
    time = now.strftime('%Y-%m-%dT%H:%M:%S')
    print('%s function took %0.3f ms' % (func.func_name, (time2-time1)*1000.0))


def printTimePY3(func, time1, time2):
    print('{:s} {:s} function took {:.3f} ms'.format(timeFormat(), func.__name__, (time2-time1)*1000.0))


def timeFormat():
    now = datetime.datetime.now()
    time = now.strftime('%Y-%m-%dT%H:%M:%S')
    return time


if PY3:
    printTime = printTimePY3
else:
    printTime = printTimePY2
