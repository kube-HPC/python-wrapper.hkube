import sys
from events import Events
from datetime import datetime
import json


def stdout_redirector():
    # capture all outputs to a log file while still printing it
    class Logger:
        events = Events()
        _buffer = []
        _sending = False
        _lineBuffer = ''

        def __init__(self, bufferSize=15):
            self.terminal = sys.stdout
            self._bufferSize = bufferSize
            self._stdout = sys.stdout
            self._stderr = sys.stderr
            sys.stdout = self
            sys.stderr = self

        def _send(self):
            self._sending = True
            try:
                tmp = self._buffer
                self._buffer = []
                self.events.on_data(tmp)
            finally:
                self._sending = False

        def write(self, message):
            self.terminal.write(message)
            self._lineBuffer += message
            if ('\n' in self._lineBuffer or '\r' in self._lineBuffer):
                lines = self._lineBuffer.splitlines()
                self._lineBuffer = ''
                now = datetime.utcnow()
                nowStr = now.strftime('%Y-%m-%dT%H:%M:%S') + now.strftime('.%f')[:4] + 'Z'
                for line in lines:
                    self._buffer.append(json.dumps({'log': line, 'stream': 'stdout', 'time': nowStr}))

            if (self.events and not self._sending and len(self._buffer) > self._bufferSize):
                self._send()

        def flush(self):
            if (self.events and not self._sending):
                self._send()

        def __getattr__(self, attr):
            return getattr(self.terminal, attr)

        def cleanup(self):
            sys.stdout = self._stdout
            sys.stderr = self._stderr

    return Logger()

    # _stdout = sys.stdout
    # _stderr = sys.stderr
    # sys.stdout = logger
    # sys.stderr = logger
    # try:
    #     yield logger
    # finally:
    #     sys.stdout = _stdout
    #     sys.stderr = _stderr
