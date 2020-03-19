from __future__ import print_function, division, absolute_import
import dpath.util
import collections


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
    return (isinstance(prop, int) and isinstance(obj, collections.Sequence)) or prop in obj


def getShallowProperty(obj, prop):
    if (hasShallowProperty(obj, prop)):
        return obj[prop]


def setPath(source, path, value):
    dpath.util.set(source, path, value)


def flatten(cls, inp, sep="/"):

    obj = collections.OrderedDict()

    def recurse(t, parent_key=""):

        if isinstance(t, list):
            for i in range(len(t)):
                recurse(t[i], parent_key + sep + str(i) if parent_key else str(i))
        elif isinstance(t, dict):
            for k, v in t.items():
                recurse(v, parent_key + sep + k if parent_key else k)
        else:
            obj[parent_key] = t

    recurse(inp)

    return obj


def createDataPath(path, index):
    dataPath = path
    if isinstance(index, six.integer_types):
        if (path is not None):
            dataPath = '{path}.{index}'.format(path=path, index=index)
        else:
            dataPath = str(index)

    return dataPath.replace(".", "/") if dataPath else None
