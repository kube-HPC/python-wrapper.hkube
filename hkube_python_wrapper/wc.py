from __future__ import print_function, division, absolute_import
from six import PY2
import websocket
from events import Events
import time
import logging
import msgpack

class WebsocketClient:
    def __init__(self, msg_queue):
        self.events = Events()
        self.msg_queue=msg_queue
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
            "subPipelineStarted": self.subPipelineStarted,
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

    def subPipelineStarted(self, data):
        self.events.on_subPipelineStarted(data)
        
    def subPipelineDone(self, data):
        self.events.on_subPipelineDone(data)

    def subPipelineError(self, data):
        self.events.on_subPipelineError(data)

    def subPipelineStopped(self, data):
        self.events.on_subPipelineStopped(data)

    def on_message(self, message):
        logging.info('got message before decode')
        decoded = msgpack.loads(message, raw=True if PY2 else False)
        command = decoded["command"]
        data = decoded.get("data", None)
        logging.info('got message from worker: {command}'.format(command=command))
        # func = self._switcher.get(command)
        # func(data)
        self.msg_queue.put((command,data))

    def on_error(self, error):
        if self._firstConnect:
            logging.error(error)

    def on_close(self):
        self.events.on_disconnect()

    def on_open(self):
        self._firstConnect = True
        self.events.on_connection()

    def send(self, message):
        logging.info('packing message')
        msgPacked = msgpack.dumps(message, use_bin_type=True)
        logging.info('sending message to worker: {command}. data length is {data_len}'.format(command=message['command'], data_len=len(msgPacked)))
        self._ws.send(msgPacked, opcode=2)

    
    def on_message_cont(self, message, flag):
        pass
    def startWS(self, url):
        self._ws = websocket.WebSocketApp(
            url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_open=self.on_open,
            on_close=self.on_close,
            on_cont_message=self.on_message_cont)

        while self._active:
            try:
                self._ws.run_forever()
                time.sleep(self._reconnectInterval)
            except:
                pass

    def stopWS(self):
        self._active = False
