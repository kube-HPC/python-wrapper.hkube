from __future__ import print_function, division, absolute_import
import gevent
from gevent import monkey
monkey.patch_all()
import websocket
from websocket import ABNF

import simplejson as json
import bson
from bson.codec_options import CodecOptions, TypeRegistry
from events import Events
import time

def fallback_encoder(value):
    if isinstance(value, bytearray):
        return bson.binary.Binary(value)
    return value

type_registry = TypeRegistry(fallback_encoder=fallback_encoder)
codec_options = CodecOptions(type_registry=type_registry)

class WebsocketClient:
    def __init__(self, binary=False):
        self.events = Events()
        self._ws = None
        self._reconnectInterval = 0.1
        self._active = True
        self._binary=binary
        self._switcher = {
            "initialize": self.init,
            "start": self.start,
            "stop": self.stop,
            "exit": self.exit,
            "algorithmExecutionDone": self.algorithmExecutionDone,
            "algorithmExecutionError": self.algorithmExecutionError,
            "subPipelineDone": self.subPipelineDone,
            "subPipelineStarted": self.subPipelineStarted,
            "subPipelineError": self.subPipelineError,
            "subPipelineStopped": self.subPipelineStopped
        }
        self._firstConnect = False
        self._encode=(lambda data: bson.encode(data,codec_options=codec_options )) if self._binary else json.dumps
        self._decode=bson.decode if self._binary else json.loads
        self._ws_opcode = ABNF.OPCODE_BINARY if self._binary else ABNF.OPCODE_TEXT
        print('Initialized socket with {encoding} encoding'.format(encoding='binary' if self._binary else 'json'))


    def init(self, data):
        self.events.on_init(data)

    def start(self, data):
        self.events.on_start(data)

    def stop(self, data):
        self.events.on_stop(data)

    def exit(self, data):
        self.events.on_exit(data)

    def algorithmExecutionDone(self, data):
        self.events.on_algorithmExecutionDone(data)

    def algorithmExecutionError(self, data):
        self.events.on_algorithmExecutionError(data)

    def subPipelineStarted(self, data):
        self.events.on_subPipelineStarted(data)
        
    def subPipelineDone(self, data):
        self.events.on_subPipelineDone(data)

    def subPipelineError(self, data):
        self.events.on_subPipelineError(data)

    def subPipelineStopped(self, data):
        self.events.on_subPipelineStopped(data)

    def on_message(self, message):
        decoded = self._decode(message)
        command = decoded["command"]
        data = decoded.get("data", None)
        print('got message from worker: {command}'.format(command=command))
        func = self._switcher.get(command)
        gevent.spawn(func,data)

    def on_error(self, error):
        if self._firstConnect:
            print(error)

    def on_close(self):
        self.events.on_disconnect()

    def on_open(self):
        self._firstConnect = True
        self.events.on_connection()

    def send(self, message):
        print('sending message to worker: {command}'.format(**message))
        self._ws.send(self._encode(message),opcode=self._ws_opcode)

    def startWS(self, url):
        self._ws = websocket.WebSocketApp(
            url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_open=self.on_open,
            on_close=self.on_close)
        while self._active:
            try:
                self._ws.run_forever()
                time.sleep(self._reconnectInterval)
            except:
                pass

    def stopWS(self):
        self._active = False
