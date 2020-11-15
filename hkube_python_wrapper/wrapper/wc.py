from __future__ import print_function, division, absolute_import
import time
from events import Events
from websocket import ABNF
import websocket
from hkube_python_wrapper.util.encoding import Encoding
from threading import Thread
from hkube_python_wrapper.wrapper.messages import messages

class WebsocketClient(Thread):
    def __init__(self, msg_queue, encoding, url):
        Thread.__init__(self, name='WebsocketClient')
        self.daemon = True
        self.events = Events()
        self._msg_queue = msg_queue
        self._ws = None
        self._reconnectInterval = 0.1
        self._active = True
        self._firstConnect = False
        self._printThrottleMessages = {
            messages.outgoing.streamingStatistics: {"delay": 30, "lastPrint": None}
        }
        self._encoding = Encoding(encoding)
        self._ws_opcode = ABNF.OPCODE_BINARY if self._encoding.isBinary else ABNF.OPCODE_TEXT
        self._url = url
        print('Initialized socket with {encoding} encoding'.format(encoding=encoding))

    def on_message(self, message):
        decoded = self._encoding.decode(value=message, plainEncode=True)
        command = decoded["command"]
        data = decoded.get("data", None)
        print('got message from worker: {command}'.format(command=command))
        self._msg_queue.put((command, data))

    def on_error(self, error):
        if self._firstConnect:
            print(error)

    def on_close(self):
        self.events.on_disconnect()

    def on_open(self):
        self._firstConnect = True
        self.events.on_connection()

    def send(self, message):
        self._printThrottle(message)
        self._ws.send(self._encoding.encode(message, plainEncode=True), opcode=self._ws_opcode)

    def _printThrottle(self, message):
        command = message["command"]
        setting = self._printThrottleMessages.get(command)
        shouldPrint = True
        if(setting):
            delay = setting["delay"]
            lastPrint = setting["lastPrint"]

            if(lastPrint is None or time.time() - lastPrint > delay):
                shouldPrint = True
                setting.update({"lastPrint": time.time()})
            else:
                shouldPrint = False

        if(shouldPrint):
            print('sending message to worker: {command}'.format(command=command))

    def run(self):
        self._startWS(self._url)

    def _startWS(self, url):
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
            except Exception:
                pass

    def shutDown(self):
        self._active = False
        if self._ws:
            self._ws.close()
