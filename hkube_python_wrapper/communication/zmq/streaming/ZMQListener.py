import zmq
import uuid
import time
import threading
from hkube_python_wrapper.util.logger import log
from hkube_python_wrapper.communication.zmq.streaming.signals import *

context = zmq.Context()
POLL_MS = 1000

class ZMQListener(object):
    def __init__(self, remoteAddress, onMessage, encoding, consumerType):
        self._encoding = encoding
        self._onMessage = onMessage
        self._consumerType = self._encoding.encode(consumerType, plainEncode=True)
        self._remoteAddress = remoteAddress
        self._active = True
        self._result = None
        self._worker = None
        self._context = None
        self._working = False
        self._worker = self._worker_socket(self._remoteAddress)

    def _worker_socket(self, remoteAddress):
        """Helper function that returns a new configured socket
           connected to the Paranoid Pirate queue"""
        identity = str(uuid.uuid4()).encode()
        worker = context.socket(zmq.DEALER)  # DEALER
        worker.setsockopt(zmq.LINGER, 0)
        worker.setsockopt(zmq.IDENTITY, identity)
        worker.connect(remoteAddress)
        log.info("zmq listener connecting to {addr}", addr=remoteAddress)
        return worker

    def _send(self, signal, result=None):
        arr = [signal, self._consumerType, result or PPP_EMPTY]
        self._worker.send_multipart(arr, copy=False)

    def _handleAMessage(self, frames):
        _, encodedMessageFlowPattern, header, message = frames  # pylint: disable=unbalanced-tuple-unpacking
        messageFlowPattern = self._encoding.decode(value=encodedMessageFlowPattern, plainEncode=True)
        return self._onMessage(messageFlowPattern, header, message)

    def fetch(self):
        self._working = True
        self._send(PPP_READY)
        result = self._worker.poll(POLL_MS)

        if result == zmq.POLLIN:
            frames = self._worker.recv_multipart()
            signal = frames[0]

            if (signal == PPP_MSG):
                msgResult = self._handleAMessage(frames)
                self._send(PPP_DONE, msgResult)
        self._working = False

    def close(self, force=True):
        if (self._active is False):
            log.warning("Attempting to close inactive ZMQListener")
        else:
            self._active = False
            while self._working and not force:
                time.sleep(1)
            self._worker.close()
