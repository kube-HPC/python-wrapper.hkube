import os
import random
from hkube_python_wrapper.util.encoding import Encoding

size = 1 * 1024

def test_none_encoding():
    encoding = Encoding('msgpack')
    decoded = encoding.decode(header=None, value=None)
    assert decoded is None

def test_json_encoding():
    encoding = Encoding('json')
    data = createObjectJson(size)
    encoded = encoding.encode(data, plainEncode=True)
    decoded = encoding.decode(value=encoded, plainEncode=True)
    assert data == decoded

def test_bson_encoding():
    encoding = Encoding('bson')
    data = createObject(size, size)
    (header, payload) = encoding.encode(data)
    decoded = encoding.decode(header=header, value=payload)
    assert data == decoded

def test_msgpack_encoding():
    encoding = Encoding('msgpack')
    data = create_bytearray(size)
    (header, payload) = encoding.encode(data)
    decoded = encoding.decode(header=header, value=payload)
    assert data == decoded

def test_encoding_header_payload_bytes():
    encoding = Encoding('msgpack')
    data = create_bytearray(size)
    (header, payload) = encoding.encode(data)
    decoded = encoding.decode(header=header, value=payload)
    assert data == decoded

def test_encoding_header_payload_object():
    encoding = Encoding('msgpack')
    data = createObject(size, size)
    (header, payload) = encoding.encode(data)
    decoded = encoding.decode(header=header, value=payload)
    assert data == decoded

def test_encoding_no_header_bytes():
    encoding = Encoding('msgpack')
    data = create_bytearray(size)
    (_, payload) = encoding.encode(data)
    decoded = encoding.decode(header=None, value=payload)
    assert data == decoded

def test_encoding_no_header_object():
    encoding = Encoding('msgpack')
    data = createObject(size, size)
    (_, payload) = encoding.encode(data)
    decoded = encoding.decode(header=None, value=payload)
    assert data == decoded

def test_encoding_header_in_payload_bytes():
    encoding = Encoding('msgpack')
    data = create_bytearray(size)
    (header, payload) = encoding.encode(data)
    decoded = encoding.decode(header=None, value=header + payload)
    assert data == decoded

def test_encoding_header_in_payload_object():
    encoding = Encoding('msgpack')
    data = createObject(size, size)
    (header, payload) = encoding.encode(data)
    decoded = encoding.decode(header=None, value=header + payload)
    assert data == decoded

def create_bytearray(sizeBytes):
    return b'\xdd' * (sizeBytes)

def randomString(n):
    min_lc = ord(b'a')
    len_lc = 26
    ba = bytearray(os.urandom(n))
    for i, b in enumerate(ba):
        ba[i] = min_lc + b % len_lc  # convert 0..255 to 97..122
    return ba.decode("utf-8")

def randomInt(sizeBytes):
    return random.sample(range(0, sizeBytes), sizeBytes)

def createObject(sizeBytes, sizeRandom):
    obj = {
        "bytesData": bytearray(b'\xdd' * (sizeBytes)),
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

def createObjectJson(sizeRandom):
    obj = {
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
