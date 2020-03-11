from __future__ import print_function, division, absolute_import
from bson.codec_options import CodecOptions, TypeRegistry
import bson
import simplejson as json


def fallback_encoder(value):
    if isinstance(value, bytearray):
        return bson.binary.Binary(value)
    return value


type_registry = TypeRegistry(fallback_encoder=fallback_encoder)
codec_options = CodecOptions(type_registry=type_registry)


class Encoding:
    def __init__(self, encoding):

        self.encode = self._bsonEncode if encoding == 'bson' else json.dumps
        self.decode = self._bsonDecode if encoding == 'bson' else json.loads

    def _bsonEncode(self, data):
        return bson.encode({"data": data}, codec_options=codec_options)

    def _bsonDecode(self, data):
        res = bson.decode(data)
        return res["data"]
