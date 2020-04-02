import os
import string
import random
from util.encoding import Encoding


def test_json_encoding():
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
        data = createObject(size * 1000000, size)
        encoded = encoding.encode(data)
        decoded = encoding.decode(encoded)
        assert data == decoded


def test_msgpack_encoding():
    encoding = Encoding('msgpack')
    sizes = [1,2,3]
    for size in sizes:
        data = createObject(size * 1000000, size)
        encoded = encoding.encode(data)
        decoded = encoding.decode(encoded)
        assert data == decoded


def test_msgpack_encodingx():
    use_bin_type_false = 'use_bin_type'
    encoding = Encoding('msgpack')
    data = {
        'taskId': 'taskid',
        'prop': 'blasds'
    }
    # encoded = encoding.encode(data)

    # with open(use_bin_type_false, 'wb') as f:
    #     f.write(encoded)

    # with open(use_bin_type_false, 'rb') as f:
    #     result = f.read()

    # decoded = encoding.decode(result)
    # assert decoded == decoded


def randomString(n):
    min_lc = ord(b'a')
    len_lc = 26
    ba = bytearray(os.urandom(n))
    for i, b in enumerate(ba):
        ba[i] = min_lc + b % len_lc  # convert 0..255 to 97..122
    return ba.decode("utf-8")


def randomInt(size):
    return random.sample(range(0, size), size)


def createObject(sizeBytes, sizeRandom):
    obj = {
        "bytesData": bytearray(b'\xdd'*(sizeBytes)),
        "anotherBytesData": bytearray(sizeBytes),
        "randomString": randomString(sizeRandom),
        "randomIntArray": randomInt(sizeRandom),
        "dataString": randomString(sizeRandom),
        "bool": False,
        "anotherBool": False,
        "nestedObj": {
            "dataString": randomString(sizeRandom),
            "randomIntArray": randomInt(sizeRandom)
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