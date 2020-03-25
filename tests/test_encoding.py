
from util.encoding import Encoding


def test_bson_encoding():
    encoding = Encoding('bson')
    size = 500
    data = bytearray(b'\xdd'*(size*1000000))
    encoded = encoding.encode(data)
    decoded = encoding.decode(encoded)
    assert data == decoded


def test_bson_encoding_sizes():
    encoding = Encoding('bson')
    sizes = [100, 200, 300]
    for size in sizes:
        data = bytearray(size * 1000000)
        encoded = encoding.encode(data)
        decoded = encoding.decode(encoded)
        assert data == decoded
