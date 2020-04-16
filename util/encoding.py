from __future__ import print_function, division, absolute_import
import sys
import bson
import struct
from bson.codec_options import CodecOptions, TypeRegistry
import simplejson as json
import msgpack
import util.type_check as typeCheck
from util.decorators import timing


def bson_fallback_encoder(value):
    if typeCheck.isBytearray(value):
        return bson.binary.Binary(value)
    return value


type_registry = TypeRegistry(fallback_encoder=bson_fallback_encoder)
codec_options = CodecOptions(type_registry=type_registry)
PY3 = sys.version_info[0] == 3


VERSION = 0x01
FOOTER_LENGTH = 0x05
UNUSED = 0x01
DATA_TYPE_RAW = 0x01
DATA_TYPE_ENCODED = 0x02

class Encoding:
    def __init__(self, encoding):
        encoders = {
            "bson": {
                "encode": self._bsonEncode,
                "decode": self._bsonDecode,
                "isBinary": True,
                "protocolType": 0x01
            },
            "json": {
                "encode": self._jsonEncode,
                "decode": self._jsonDecode,
                "isBinary": False,
                "protocolType": 0x02
            },
            "msgpack": {
                "encode": self._msgpackEncode,
                "decode": self._msgpackDecode,
                "isBinary": True,
                "protocolType": 0x03
            }
        }
        encoder = encoders[encoding]
        self.type = encoding
        self._encode = encoder["encode"]
        self._decode = encoder["decode"]
        self.isBinary = encoder["isBinary"]
        self.protocolType = encoder["protocolType"]

    @timing
    def encode(self, value, **kwargs):
        plainEncode = kwargs.get('plain_encode')
        if(self.isBinary is False or plainEncode is True):
            return self._encode(value)

        payload = None
        if (typeCheck.isBytearray(value)):
            dataType = DATA_TYPE_RAW
            payload = value
        else:
            dataType = DATA_TYPE_ENCODED
            payload = self._encode(value)

        footer = self.creaetFooter(dataType, self.protocolType)
        payload += footer
        return payload

    @timing
    def decode(self, value, **kwargs):
        plainEncode = kwargs.get('plain_encode')
        if(self.isBinary is False or plainEncode is True):
            return self._decode(value)

        view = memoryview(value)
        totalLength = len(view)
        footer = bytes(view[-5:])
        ver = bytes(footer[0:1])
        hel = bytes(footer[1:2])
        dt = bytes(footer[3:4])
        dataType = struct.unpack(">B", dt)[0]
        footerLength = struct.unpack(">B", hel)[0]
        data = view[0: totalLength - footerLength]

        payload = None
        if(dataType == DATA_TYPE_ENCODED):
            payload = self._decode(data)
        else:
            payload = data.tobytes()
        return payload

    def _bsonEncode(self, value):
        return bson.encode({'data': value}, codec_options=codec_options)

    def _bsonDecode(self, value):
        res = bson.decode(value)
        return res.get("data")

    def _bsonEncode(self, value):
        return bson.encode({'data': value}, codec_options=codec_options)

    def _jsonEncode(self, value):
        return json.dumps(value)

    def _jsonDecode(self, value):
        return json.loads(value)

    @timing
    def _msgpackEncode(self, value):
        return msgpack.packb(value, use_bin_type=True if PY3 else False)

    @timing
    def _msgpackDecode(self, value):
        return msgpack.unpackb(value, raw=False if PY3 else True)

    def creaetFooter(self, dataType, protocolType):
        footer = bytearray()
        footer.append(VERSION)
        footer.append(FOOTER_LENGTH)
        footer.append(protocolType)
        footer.append(dataType)
        footer.append(UNUSED)
        return footer