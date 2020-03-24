from __future__ import print_function, division, absolute_import
import util.type_check as typeCheck
from util.decorators import timing


@timing
def getPath(obj, path, defaultValue="DEFAULT"):

    if (path is None or len(path) == 0):
        return obj

    if (obj is None):
        return defaultValue

    if (typeCheck.isString(path)):
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


@timing
def setPath(obj, path, value, doNotReplace=False):
    if (typeCheck.isInt(path)):
        path = [path]

    if (path is None or len(path) == 0):
        return obj

    if (typeCheck.isString(path)):
        return setPath(obj, list(map(getKey, path.split('.'))), value, doNotReplace)

    currentPath = path[0]
    currentValue = getShallowProperty(obj, currentPath)
    if (len(path) == 1):
        if (currentValue is None or doNotReplace == False):
            obj[currentPath] = value
        return currentValue

    if (currentValue is None):
        if(typeCheck.isInt(path[1])):
            obj[currentPath] = []
        else:
            obj[currentPath] = {}

    return setPath(obj[currentPath], path[1:], value, doNotReplace)
