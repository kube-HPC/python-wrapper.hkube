from __future__ import print_function, division, absolute_import
import sys
import bson
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


class Encoding:
    def __init__(self, encoding):
        encoders = {
            "bson": {
                "encode": self._bsonEncode,
                "decode": self._bsonDecode,
                "isBinary": True
            },
            "json": {
                "encode": self._jsonEncode,
                "decode": self._jsonDecode,
                "isBinary": False
            },
            "msgpack": {
                "encode": self._msgpackEncode,
                "decode": self._msgpackDecode,
                "isBinary": True
            }
        }
        encoder = encoders[encoding]
        self.type = encoding
        self.encode = encoder["encode"]
        self.decode = encoder["decode"]
        self.isBinary = encoder["isBinary"]

    @timing
    def _bsonDecode(self, data):
        res = bson.decode(data)
        return res.get("data")

    @timing
    def _bsonEncode(self, data):
        return bson.encode({'data': data}, codec_options=codec_options)

    @timing
    def _jsonEncode(self, *args):
        return json.dumps(*args)

    @timing
    def _jsonDecode(self, *args):
        return json.loads(*args)

    @timing
    def _msgpackEncode(self, *args):
        return msgpack.packb(*args, use_bin_type=True if PY3 else False)

    @timing
    def _msgpackDecode(self, *args):
        return msgpack.unpackb(*args, raw=False if PY3 else True)
