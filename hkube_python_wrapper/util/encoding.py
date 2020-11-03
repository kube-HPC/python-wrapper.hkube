from __future__ import print_function, division, absolute_import
import sys
import base64
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
- Include 2 bytes for magic number and 2 reserved bytes

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

    def encode(self, value, useHeader=True, plainEncode=False):
        if (not self.isBinary or not useHeader or plainEncode is True):
            return self._encode(value)
        if (typeCheck.isBytearray(value)):
            dataType = DATA_TYPE_RAW
            payload = value
        else:
            dataType = DATA_TYPE_ENCODED
            payload = self._encode(value)

        header = self.createHeader(dataType, self.protocolType)
        return (header, payload)

    def decode(self, header=None, value=None, plainEncode=False):
        if (not self.isBinary or plainEncode is True):
            return self._decode(value)

        if (not typeCheck.isBytearray(value)):
            return value

        if(header is None):
            # try to extract header and payload
            header = value[0:HEADER_LENGTH]
            if(self.isHeader(header)):
                totalLength = len(value)
                value = value[HEADER_LENGTH: totalLength]

        if(not self.isHeader(header)):
            try:
                payload = self._decode(value)
            except Exception:
                payload = value
            return payload

        dt = bytes(header[2:3])
        dataType = struct.unpack(">B", dt)[0]
        if (dataType == DATA_TYPE_ENCODED):
            payload = self._decode(value)
        else:
            payload = value
        return payload

    def isHeader(self, header):
        if(header is None):
            return False
        mg = bytes(header[-2:])
        return mg == MAGIC_NUMBER

    def headerLength(self):
        return HEADER_LENGTH

    @staticmethod
    def headerToString(value):
        encoded = base64.b64encode(value)
        return encoded.decode('utf-8')

    @staticmethod
    def headerFromString(value):
        if(value is None):
            return None
        decoded = base64.b64decode(value)
        return decoded

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
        return msgpack.packb(value, use_bin_type=True)

    def _msgpackDecode(self, value):
        return msgpack.unpackb(value, raw=False)

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
