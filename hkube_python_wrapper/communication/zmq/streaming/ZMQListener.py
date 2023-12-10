import zmq
import uuid
import time
from hkube_python_wrapper.util.logger import log
from hkube_python_wrapper.communication.zmq.streaming import signals

context = zmq.Context()
POLL_MS = 1000
MAX_POLLS = 5

class ZMQListener(object):
    def __init__(self, remoteAddress, onMessage, encoding, consumerType):
        self._encoding = encoding
        self._onMessage = onMessage
        self._consumerType = self._encoding.encode(consumerType, plainEncode=True)
        self._active = True
        self._working = True
        self._pollTimeoutCount = 0
        self._remoteAddress = remoteAddress
        self._worker = self._worker_socket(remoteAddress)

    def _worker_socket(self, remoteAddress):
        """Helper function that returns a new configured socket
           connected to the Paranoid Pirate queue"""
        identity = str(uuid.uuid4()).encode()
        worker = context.socket(zmq.DEALER)
        worker.setsockopt(zmq.IDENTITY, identity)
        worker.connect(remoteAddress)
        log.info("zmq listener connecting to {addr}", addr=remoteAddress)
        return worker

    def _send(self, signal, result=None):
        arr = [signal, self._consumerType, result or signals.PPP_EMPTY]
        self._worker.send_multipart(arr, copy=False)

    def _handleAMessage(self, frames):
        _, encodedMessageFlowPattern, header, message = frames  # pylint: disable=unbalanced-tuple-unpacking
        messageFlowPattern = self._encoding.decode(value=encodedMessageFlowPattern, plainEncode=True)
        return self._onMessage(messageFlowPattern, header, message)

    def fetch(self):
        try:
            if (self._active is False):
                time.sleep(0.2)
                return

            if (self._pollTimeoutCount == MAX_POLLS):
                log.warning('ZMQListener poll timeout reached')
                self._pollTimeoutCount = 0
                self._worker.close()
                self._worker = self._worker_socket(self._remoteAddress)

            if (self._pollTimeoutCount > 0):
                self._readMessage()
                return

            self._send(signals.PPP_READY)
            self._readMessage()

        except Exception as e:
            log.error('ZMQListener.fetch {e}', e=str(e))
        finally:
            if(self._active is False):
                self._working = False

    def _readMessage(self, timeout=POLL_MS):
        hasMsg = False
        result = self._worker.poll(timeout)
        if (result == zmq.POLLIN):
            self._pollTimeoutCount = 0
            frames = self._worker.recv_multipart()
            signal = frames[0]

            if (signal == signals.PPP_MSG):
                hasMsg = True
                msgResult = self._handleAMessage(frames)
                self._send(signals.PPP_DONE, msgResult)
            else:
                time.sleep(0.005)
        else:
            self._pollTimeoutCount += 1
            log.warning('ZMQListener poll timeout {count}', count=self._pollTimeoutCount)
        return hasMsg

    def close(self, force=True):
        closed = False
        if (self._active is False):
            log.warning('attempting to close inactive ZMQListener')
        else:
            self._active = False
            while self._working and not force:
                time.sleep(0.02)

            if (self._pollTimeoutCount):
                log.warning('trying to read message from socket after close')
                hasMsg = self._readMessage(timeout=POLL_MS * 5)
                if (hasMsg):
                    log.warning('success reading message from socket after close')

            self._worker.close()
            closed = True
        return closed
