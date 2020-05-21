from __future__ import print_function, division, absolute_import
import hkube_python_wrapper.util.type_check as typeCheck


def getPath(obj, path, defaultValue="DEFAULT"):

    if (not path):
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
    return None


def setPath(obj, path, value, doNotReplace=False):
    if (typeCheck.isInt(path)):
        path = [path]

    if (not path):
        return obj

    if (typeCheck.isString(path)):
        return setPath(obj, list(map(getKey, path.split('.'))), value, doNotReplace)

    currentPath = path[0]
    currentValue = getShallowProperty(obj, currentPath)
    if (len(path) == 1):
        if (currentValue is None or doNotReplace is False):
            obj[currentPath] = value
        return currentValue

    if (currentValue is None):
        if(typeCheck.isInt(path[1])):
            obj[currentPath] = []
        else:
            obj[currentPath] = {}

    return setPath(obj[currentPath], path[1:], value, doNotReplace)
