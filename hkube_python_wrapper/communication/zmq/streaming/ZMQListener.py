import zmq
import uuid
import time
from hkube_python_wrapper.util.logger import log
import hkube_python_wrapper.communication.zmq.streaming.signals as signals

context = zmq.Context()
POLL_MS = 1000
MIN_TIME_OUT = 1
MAX_TIME_OUT = 32

class ZMQListener(object):
    def __init__(self, remoteAddress, onMessage, encoding, consumerType):
        self._encoding = encoding
        self._onMessage = onMessage
        self._consumerType = self._encoding.encode(consumerType, plainEncode=True)
        self._active = True
        self._working = True
        self._timeout = MIN_TIME_OUT
        self._worker = self._worker_socket(remoteAddress)

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

            self._send(signals.PPP_READY)
            result = self._worker.poll(POLL_MS)

            if (result == zmq.POLLIN):
                frames = self._worker.recv_multipart()
                signal = frames[0]

                if (signal == signals.PPP_MSG):
                    self._timeout = MIN_TIME_OUT
                    msgResult = self._handleAMessage(frames)
                    self._send(signals.PPP_DONE, msgResult)
                else:
                    time.sleep(0.005)
                    # time.sleep(self._timeout / 1000)
                    # if(self._timeout < MAX_TIME_OUT):
                    #     self._timeout *= 2

        except Exception as e:
            log.error('Exception in ZMQListener.fetch {e}', e=str(e))
        finally:
            if(self._active is False):
                self._working = False

    def close(self, force=True):
        if (self._active is False):
            log.warning("Attempting to close inactive ZMQListener")
        else:
            self._active = False
            while self._working and not force:
                time.sleep(0.2)
            self._worker.close()
