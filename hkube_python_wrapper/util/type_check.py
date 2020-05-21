from __future__ import print_function, division, absolute_import
import six


def isList(value):
    return isinstance(value, list)


def isString(value):
    return isinstance(value, six.string_types)


def isDict(value):
    return isinstance(value, dict)


def isInt(value):
    return isinstance(value, six.integer_types)


def isBytearray(value):
    return isinstance(value, (bytes, bytearray))
