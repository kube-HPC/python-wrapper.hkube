import os
import string
import random
from util.encoding import Encoding


def test_json_encoding_sizes():
    encoding = Encoding('json')
    sizes = [100, 200, 300]
    for size in sizes:
        data = createObjectJson(size)
        encoded = encoding.encode(data)
        decoded = encoding.decode(encoded)
        assert data == decoded


def test_bson_encoding():
    encoding = Encoding('bson')
    sizes = [100, 200, 300]
    for size in sizes:
        data = createObject(size)
        encoded = encoding.encode(data)
        decoded = encoding.decode(encoded)
        assert data == decoded


def test_msgpack_encoding_sizes():
    encoding = Encoding('msgpack')
    sizes = [100, 200, 300]
    for size in sizes:
        data = createObject(size)
        encoded = encoding.encode(data)
        decoded = encoding.decode(encoded)
        assert data == decoded


def randomString(n):
    min_lc = ord(b'a')
    len_lc = 26
    ba = bytearray(os.urandom(n))
    for i, b in enumerate(ba):
        ba[i] = min_lc + b % len_lc  # convert 0..255 to 97..122
    return ba.decode("utf-8")


def randomInt(size):
    return random.sample(range(0, size), size)


def createObject(size):
    obj = {
        "bytesData": bytearray(b'\xdd'*(size)),
        "anotherBytesData": bytearray(size),
        "randomString": randomString(size),
        "randomIntArray": randomInt(size),
        "dataString": randomString(size),
        "bool": False,
        "anotherBool": False,
        "nestedObj": {
            "dataString": randomString(size),
            "randomIntArray": randomInt(size)
        }
    }
    return obj


def createObjectJson(size):
    obj = {
        "randomString": randomString(size),
        "randomIntArray": randomInt(size),
        "dataString": randomString(size),
        "bool": False,
        "anotherBool": False,
        "nestedObj": {
            "dataString": randomString(size),
            "randomIntArray": randomInt(size)
        }
    }
    return obj
