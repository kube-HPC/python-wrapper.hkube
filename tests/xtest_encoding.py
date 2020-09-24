import os
import random
from hkube_python_wrapper.util.encoding import Encoding
from hkube_python_wrapper.storage.storage_manager import StorageManager
from tests.configs import config
from hkube_python_wrapper.util.decorators import timing


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


encoding = Encoding('msgpack')

storageManager = StorageManager(config.storage)


@timing
def create_bytearray(sizeBytes):
    return bytearray(b'\xdd'*(sizeBytes))


def test_msgpack_bytearray_storage(sizeBytes):
    data = create_bytearray(sizeBytes)

    storageManager.storage.put({"path": "tests/file2", "data": data})
    (header, payload) = storageManager.storage.get({"path": "tests/file2"})

    data2 = data[0: sizeBytes]
    assert data2 == res

    return data2


def test_msgpack_bytearray(sizeBytes):
    data = create_bytearray(sizeBytes)

    # data = createObject(150, 50)

    encoded = encoding.encode(data)
    leng = len(encoded)

    # assert data == encoded

    decoded = encoding.decode(encoded)

    # data2 = data[0: sizeBytes]
    # assert data2 == decoded

    return encoded


mb = 1024 * 1024

sizes = [10]

for size in sizes:
    test_msgpack_bytearray(size)
