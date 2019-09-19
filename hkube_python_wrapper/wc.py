from __future__ import print_function, division, absolute_import
import gevent
from gevent import monkey
monkey.patch_all()
import websocket
import simplejson as json
from events import Events
import time


class WebsocketClient:
    def __init__(self):
        self.events = Events()
        self._ws = None
        self._reconnectInterval = 0.1
        self._active = True
        self._switcher = {
            "initialize": self.init,
            "start": self.start,
            "stop": self.stop,
            "exit": self.exit,
            "algorithmExecutionDone": self.algorithmExecutionDone,
            "algorithmExecutionError": self.algorithmExecutionError,
            "subPipelineDone": self.subPipelineDone,
            "subPipelineError": self.subPipelineError,
            "subPipelineStopped": self.subPipelineStopped
        }
        self._firstConnect = False

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

    def subPipelineDone(self, data):
        self.events.on_subPipelineDone(data)

    def subPipelineError(self, data):
        self.events.on_subPipelineError(data)

    def subPipelineStopped(self, data):
        self.events.on_subPipelineStopped(data)

    def on_message(self, message):
        decoded = json.loads(message)
        command = decoded["command"]
        data = decoded.get("data", None)
        print('got message from worker: {command}, data: {data}'.format(command=command, data=data))
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
        self._ws.send(json.dumps(message))

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
