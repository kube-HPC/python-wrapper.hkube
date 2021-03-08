import time
import zmq
import uuid
import threading
import hkube_python_wrapper.communication.zmq.streaming.signals as signals
from hkube_python_wrapper.util.logger import log

CYCLE_LENGTH_MS = 1000
HEARTBEAT_INTERVAL = 10
HEARTBEAT_LIVENESS_TIMEOUT = 30
INTERVAL_INIT = 1
INTERVAL_MAX = 32

lock = threading.Lock()

class ZMQListener(object):
    """ZMQListener

    This class runs in separate thread, but only one single thread can
    proccess data at each moment, this is dramatically decreade performance!!

    """
    def __init__(self, remoteAddress, onMessage, encoding, consumerType, onReady=None, onNotReady=None):
        self._encoding = encoding
        self._onMessage = onMessage
        self._onReady = onReady
        self._onNotReady = onNotReady
        self._consumerType = self._encoding.encode(consumerType, plainEncode=True)
        self._remoteAddress = remoteAddress
        self._active = True
        self._worker = None
        self._context = None
        self._interval = INTERVAL_INIT
        self._lastSentTime = time.time()
        self._lastReceiveTime = time.time()
        self._ready = None
        self._readySent = None
        self._notReadySent = None

    def _worker_socket(self, remoteAddress, context=None):
        """Helper function that returns a new configured socket
           connected to the Paranoid Pirate queue"""
        if not context:
            context = zmq.Context(1)
        self._context = context
        identity = str(uuid.uuid4()).encode()
        worker = self._context.socket(zmq.DEALER)  # DEALER
        worker.setsockopt(zmq.IDENTITY, identity)
        worker.setsockopt(zmq.LINGER, 0)
        worker.connect(remoteAddress)
        log.info("zmq listener connecting to {addr}", addr=remoteAddress)
        self._send(worker, signals.PPP_INIT)
        return worker

    def _send(self, worker, signal, result=signals.PPP_EMPTY):
        if(worker and (self._active or signal == signals.PPP_DISCONNECT)):
            arr = [signal, self._consumerType, result]
            worker.send_multipart(arr, copy=False)
            self._lastSentTime = time.time()

    def _handleAMessage(self, frames):
        _, encodedMessageFlowPattern, header, message = frames  # pylint: disable=unbalanced-tuple-unpacking
        messageFlowPattern = self._encoding.decode(value=encodedMessageFlowPattern, plainEncode=True)
        return self._onMessage(messageFlowPattern, header, message)

    def start(self):  # pylint: disable=too-many-branches
        self._worker = self._worker_socket(self._remoteAddress)

        while self._active: # pylint: disable=too-many-nested-blocks
            try:
                result = self._worker.poll(CYCLE_LENGTH_MS)
                lock.acquire()

                if result == zmq.POLLIN:
                    frames = self._worker.recv_multipart()

                    if not frames:
                        raise Exception("Connection to producer on " + self._remoteAddress + " interrupted")

                    signal = frames[0]

                    if (signal == signals.PPP_MSG):
                        self.onNotReady()
                        result = self._handleAMessage(frames)
                        self.onReady()
                        self._send(self._worker, signals.PPP_DONE, result)

                    self._interval = INTERVAL_INIT
                    self._lastReceiveTime = time.time()
                else:
                    if (time.time() - self._lastReceiveTime > HEARTBEAT_LIVENESS_TIMEOUT):
                        self._lastReceiveTime = time.time()
                        log.warning("Heartbeat failure {addr}, Reconnecting in {interval:0.2f}", addr=str(self._remoteAddress), interval=self._interval)

                        if (self._interval < INTERVAL_MAX):
                            self._interval *= 2

                        if (self._active):
                            self._worker.close()
                            self._worker = self._worker_socket(self._remoteAddress, self._context)

                    else:
                        self._sendHeartBeat()

            except Exception as e:
                if (self._active):
                    log.error('Error in ZMQListener {e}', e=str(e))
                    raise e
                break

            finally:
                if(lock.locked()):
                    lock.release()

    def _sendHeartBeat(self):
        if (time.time() - self._lastSentTime > HEARTBEAT_INTERVAL):
            self._send(self._worker, signals.PPP_HEARTBEAT)

    def ready(self):
        self._ready = True
        if(not self._readySent):
            self._readySent = True
            self._notReadySent = False
            self._send(self._worker, signals.PPP_READY)

    def notReady(self):
        self._ready = False
        if (not self._notReadySent):
            self._notReadySent = True
            self._readySent = False
            self._send(self._worker, signals.PPP_NOT_READY)

    def onReady(self):
        if(self._onReady):
            self._onReady(self._remoteAddress)

    def onNotReady(self):
        if(self._onNotReady):
            self._onNotReady(self._remoteAddress)

    def close(self, force=True):
        # pylint: disable=too-many-nested-blocks
        if (self._active is False):
            log.warning("Attempting to close inactive ZMQListener")
        else:
            self._active = False
            if (self._worker is not None):
                if (force is False):
                    try:
                        readAfterStopped = 0
                        result = self._worker.poll(CYCLE_LENGTH_MS * 5)
                        while result == zmq.POLLIN:
                            lock.acquire()
                            try:
                                frames = self._worker.recv_multipart()
                                signal = frames[0]
                                if (signal == signals.PPP_MSG):
                                    self._handleAMessage(frames)
                                    readAfterStopped += 1
                                    log.warning('Read after stop {readAfterStopped}', readAfterStopped=readAfterStopped)
                                    self._send(self._worker, signals.PPP_DISCONNECT)
                                    break
                            finally:
                                lock.release()
                            result = self._worker.poll(CYCLE_LENGTH_MS)
                    except Exception as e:
                        log.error('Error on zmqListener close {e}', e=str(e))
                else:
                    try:
                        self._send(self._worker, signals.PPP_DISCONNECT)
                    except Exception as e:
                        log.error('Error on sending disconnect {e}', e=str(e))

                self._worker.close()
                self._context.destroy()
