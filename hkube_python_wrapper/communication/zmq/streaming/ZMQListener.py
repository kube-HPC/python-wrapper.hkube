import zmq
import uuid
from hkube_python_wrapper.util.logger import log
from hkube_python_wrapper.communication.zmq.streaming.signals import *

CYCLE_LENGTH_MS = 1000

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
        self._worker = self._worker_socket(self._remoteAddress)

    def _worker_socket(self, remoteAddress, context=None):
        """Helper function that returns a new configured socket
           connected to the Paranoid Pirate queue"""
        if not context:
            context = zmq.Context(1)
        self._context = context
        worker = self._context.socket(zmq.DEALER)  # DEALER
        worker.setsockopt(zmq.LINGER, 0)
        identity = str(uuid.uuid4()).encode()
        worker.setsockopt(zmq.IDENTITY, identity)
        worker.setsockopt(zmq.LINGER, 0)
        worker.connect(remoteAddress)
        log.info("zmq listener connecting to {addr}", addr=remoteAddress)
        return worker

    def _send(self, signal):
        arr = [signal, self._consumerType, self._result or PPP_EMPTY]
        self._worker.send_multipart(arr, copy=False)

    def _handleAMessage(self, frames):
        _, encodedMessageFlowPattern, header, message = frames  # pylint: disable=unbalanced-tuple-unpacking
        messageFlowPattern = self._encoding.decode(value=encodedMessageFlowPattern, plainEncode=True)
        return self._onMessage(messageFlowPattern, header, message)

    def fetch(self):
        self._send(PPP_READY)
        result = self._worker.poll(CYCLE_LENGTH_MS)

        if result == zmq.POLLIN:
            frames = self._worker.recv_multipart()
            signal = frames[0]

            if (signal == PPP_MSG):
                self._result = self._handleAMessage(frames)
            else:
                self._result = None
        else:
            self._result = None
        
        if(self._active is False):
            self._worker.close()
            self._context.destroy()

    def close(self, force=True):
        if (self._active is False):
            log.warning("Attempting to close inactive ZMQListener")
        else:
            self._active = False
