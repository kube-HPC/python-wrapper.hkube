from __future__ import print_function, division, absolute_import
import sys
import struct
import bson
from bson.codec_options import CodecOptions, TypeRegistry
import simplejson as json
import msgpack
import hkube_python_wrapper.util.type_check as typeCheck


def bson_fallback_encoder(value):
    if typeCheck.isBytearray(value):
        return bson.binary.Binary(value)
    return value


type_registry = TypeRegistry(fallback_encoder=bson_fallback_encoder)
codec_options = CodecOptions(type_registry=type_registry)
PY3 = sys.version_info[0] == 3


'''

- Hkube header format: 8 bytes (64 bit)
- Include 2 bytes for magic number and reserved 2 bytes

+---------------------------------------------------------------+
 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7
+---------------------------------------  ----------------------+
|     VERSION    | HEADER_LENGTH  |  DATA_TYPE  | PROTOCOL_TYPE |
+---------------------------------------------------------------+
|      UNUSED    |     UNUSED     |        MAGIC NUMBER         |
+---------------------------------------------------------------+
|                         Payload Data                          |
+---------------------------------------------------------------+
'''

VERSION = 0x01
HEADER_LENGTH = 0x08
PROTOCOL_TYPE_BSON = 0x01
PROTOCOL_TYPE_JSON = 0x02
PROTOCOL_TYPE_MSGPACK = 0x03
DATA_TYPE_RAW = 0x01
DATA_TYPE_ENCODED = 0x02
UNUSED = 0x00
MAGIC_NUMBER = b'HK'


class Encoding:
    def __init__(self, encoding):
        encoders = {
            "bson": {
                "encode": self._bsonEncode,
                "decode": self._bsonDecode,
                "isBinary": True,
                "protocolType": PROTOCOL_TYPE_BSON
            },
            "json": {
                "encode": self._jsonEncode,
                "decode": self._jsonDecode,
                "isBinary": False,
                "protocolType": PROTOCOL_TYPE_JSON
            },
            "msgpack": {
                "encode": self._msgpackEncode,
                "decode": self._msgpackDecode,
                "isBinary": True,
                "protocolType": PROTOCOL_TYPE_MSGPACK
            }
        }
        encoder = encoders[encoding]
        self.type = encoding
        self._encode = encoder["encode"]
        self._decode = encoder["decode"]
        self.isBinary = encoder["isBinary"]
        self.protocolType = encoder["protocolType"]
        self._fromBytes = self._fromBytesPY3 if PY3 else self._fromBytesPY2
        self._toBytes = self._toBytesPY3 if PY3 else self._toBytesPY2

    def encode(self, value, **kwargs):
        plainEncode = kwargs.get('plain_encode')
        if(not self.isBinary or plainEncode is True):
            return self._encode(value)

        payload = None
        if (typeCheck.isBytearray(value)):
            dataType = DATA_TYPE_RAW
            payload = value
        else:
            dataType = DATA_TYPE_ENCODED
            payload = self._encode(value)

        header = self.createHeader(dataType, self.protocolType)
        header += payload
        return header

    def decode(self, value, **kwargs):
        plainEncode = kwargs.get('plain_encode')

        if(not self.isBinary or plainEncode is True):
            return self._decode(value)

        if (not typeCheck.isBytearray(value)):
            return value

        view = self._fromBytes(value)
        totalLength = len(view)
        header = bytes(view[0:HEADER_LENGTH])
        mg = bytes(header[-2:])

        if(mg != MAGIC_NUMBER):
            return self._decode(value)

        ftl = bytes(header[1:2])
        dt = bytes(header[2:3])
        headerLength = struct.unpack(">B", ftl)[0]
        dataType = struct.unpack(">B", dt)[0]
        data = view[headerLength: totalLength]

        payload = None
        if(dataType == DATA_TYPE_ENCODED):
            payload = self._decode(data)
        else:
            payload = self._toBytes(data)
        return payload

    def _fromBytesPY2(self, value):
        return value

    def _fromBytesPY3(self, value):
        return memoryview(value)

    def _toBytesPY2(self, value):
        return value

    def _toBytesPY3(self, value):
        return value.tobytes()

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

    def _msgpackEncode(self, value):
        return msgpack.packb(value, use_bin_type=bool(PY3))

    def _msgpackDecode(self, value):
        return msgpack.unpackb(value, raw=not PY3)

    def createHeader(self, dataType, protocolType):
        header = bytearray()
        header.append(VERSION)
        header.append(HEADER_LENGTH)
        header.append(dataType)
        header.append(protocolType)
        header.append(UNUSED)
        header.append(UNUSED)
        header.extend(MAGIC_NUMBER)
        return header
