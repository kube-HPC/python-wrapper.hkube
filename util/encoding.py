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


'''

- Hkube footer format: 8 bytes (64 bit)
- Include 2 bytes for magic number and reserved 2 bytes 

+---------------------------------------------------------------+
 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7
+---------------------------------------  ----------------------+
|     VERSION    | PROTOCOL_TYPE |   DATA_TYPE   |    UNUSED    |
+---------------------------------------------------------------+
|      UNUSED    | FOOTER_LENGTH |          MAGIC NUMBER        |
+---------------------------------------------------------------+
|                         Payload Data                          |
+---------------------------------------------------------------+
'''

VERSION = 0x01
FOOTER_LENGTH = 0x08
PROTOCOL_TYPE_BSON = 0x01
PROTOCOL_TYPE_JSON = 0x02
PROTOCOL_TYPE_MSGPACK = 0x03
DATA_TYPE_RAW = 0x01
DATA_TYPE_ENCODED = 0x02
UNUSED = 0x00
MAGIC_NUMBER1 = 0x48 #H
MAGIC_NUMBER2 = 0x4b #K

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

    @timing
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

        footer = self.createFooter(dataType, self.protocolType)
        payload += footer
        return payload

    def decode(self, value, **kwargs):
        plainEncode = kwargs.get('plain_encode')
        if(not self.isBinary or plainEncode is True):
            return self._decode(value)

        view = self._fromBytes(value)
        totalLength = len(view)
        mg = bytes(view[-2:])

        if(mg != b'HK'):
            return self._decode(value)
        
        print('found magic number')
        ftl = bytes(view[-3:-2])
        footerLength = struct.unpack(">B", ftl)[0]
        footer = bytes(view[-footerLength:])
        ver = bytes(footer[0:1])
        pt = bytes(footer[1:2])
        dt = bytes(footer[2:3])
       
        dataType = struct.unpack(">B", dt)[0]
        data = view[0: totalLength - footerLength]

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

    @timing
    def _msgpackEncode(self, value):
        return msgpack.packb(value, use_bin_type=True if PY3 else False)

    @timing
    def _msgpackDecode(self, value):
        return msgpack.unpackb(value, raw=False if PY3 else True)

    def createFooter(self, dataType, protocolType):
        footer = bytearray()
        footer.append(VERSION)
        footer.append(protocolType)
        footer.append(dataType)
        footer.append(UNUSED)
        footer.append(UNUSED)
        footer.append(FOOTER_LENGTH)
        footer.append(MAGIC_NUMBER1)
        footer.append(MAGIC_NUMBER2)
        return footer
