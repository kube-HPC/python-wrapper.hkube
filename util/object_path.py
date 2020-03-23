from __future__ import print_function, division, absolute_import
import dpath.util
import util.type_check as typeCheck


def getPath(obj, path, defaultValue="DEFAULT"):

    if (path is None or len(path) == 0):
        return obj

    if (obj is None):
        return defaultValue

    if (str(type(path).__name__) == 'str'):
        return getPath(obj, path.split('.'), defaultValue)

    currentPath = getKey(path[0])
    nextObj = getShallowProperty(obj, currentPath)
    if (nextObj is None):
        return defaultValue

    if (len(path) == 1):
        return nextObj

    return getPath(obj[currentPath], path[1:], defaultValue)


def getKey(key):
    value = key
    try:
        value = int(key)
    except ValueError:
        pass
    return value


def hasShallowProperty(obj, prop):
    return (typeCheck.isInt(prop) and typeCheck.isList(obj)) or prop in obj


def getShallowProperty(obj, prop):
    if (hasShallowProperty(obj, prop)):
        return obj[prop]


def setPath(source, path, value):
    dpath.util.set(source, path, value, separator=".")
