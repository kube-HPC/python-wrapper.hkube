from __future__ import print_function, division, absolute_import
import dpath.util


def getPath(value, path, separator='/'):
    result = None
    try:
        if (path):
            result = dpath.util.get(value, path, separator)
        else:
            result = value
    except Exception as e:
        result = value
    return result


def setPath(source, path, value):
    dpath.util.set(source, path, value)
