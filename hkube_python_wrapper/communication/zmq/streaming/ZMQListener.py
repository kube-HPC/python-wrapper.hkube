import time
import zmq
import uuid
import threading
import hkube_python_wrapper.communication.zmq.streaming.signals as signals
from hkube_python_wrapper.util.logger import log

POLL_TIMEOUT_MS = 1
STOP_TIMEOUT_MS = 5000
HEARTBEAT_INTERVAL = 10
HEARTBEAT_LIVENESS_TIMEOUT = 30
INTERVAL_INIT = 1
INTERVAL_MAX = 32

context = zmq.Context()
lock = threading.Lock()

class ZMQListener(object):
    """ZMQListener
        This class runs in separate thread, but only one single thread can
        proccess data at each moment, this is dramatically decreade performance!!
    """
    def __init__(self, remoteAddress, consumerType, nodeName, onMessage, encoding, onReady=None, onNotReady=None):
        self._encoding = encoding
        self._onMessage = onMessage
        self._onReady = onReady
        self._onNotReady = onNotReady
        self._nodeName = nodeName
        self._consumerType = self._encoding.encode(consumerType, plainEncode=True)
        self._remoteAddress = remoteAddress
        self._active = True
        self._closeForce = False
        self._worker = None
        self._interval = INTERVAL_INIT
        self._lastSentTime = time.time()
        self._lastReceiveTime = time.time()
        self._ready = None
        self._readySent = False
        self._notReadySent = False
        self._isClosed = False

    def _worker_socket(self, remoteAddress):
        """Helper function that returns a new configured socket
           connected to the Paranoid Pirate queue"""
        identity = str(uuid.uuid4()).encode()
        worker = context.socket(zmq.DEALER)  # DEALER
        worker.setsockopt(zmq.IDENTITY, identity)
        worker.setsockopt(zmq.LINGER, 0)
        worker.connect(remoteAddress)
        self._send(worker, signals.PPP_INIT)
        return worker

    def _send(self, worker, signal, result=signals.PPP_EMPTY):
        if(worker):
            arr = [signal, self._consumerType, result]
            worker.send_multipart(arr, copy=False)
            self._lastSentTime = time.time()

    def _handleAMessage(self, frames):
        _, encodedMessageFlowPattern, header, message = frames  # pylint: disable=unbalanced-tuple-unpacking
        messageFlowPattern = self._encoding.decode(value=encodedMessageFlowPattern, plainEncode=True)
        return self._onMessage(messageFlowPattern, header, message)

    def start(self):  # pylint: disable=too-many-branches
        log.info("start receiving from node {node} in address {address}", node=self._nodeName, address=self._remoteAddress)
        self._worker = self._worker_socket(self._remoteAddress)

        while self._active: # pylint: disable=too-many-nested-blocks
            try:
                lockRes = lock.acquire(blocking=False)
                if(lockRes is False):
                    self._checkReady()
                    continue

                result = self._worker.poll(POLL_TIMEOUT_MS)
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
                        log.warning("Heartbeat failure {addr}, Reconnecting in {interval:0.2f}", addr=self._remoteAddress, interval=self._interval)

                        if (self._interval < INTERVAL_MAX):
                            self._interval *= 2

                        if (self._active):
                            log.info("reconnecting to node {node} in address {address}", node=self._nodeName, address=self._remoteAddress)
                            self._worker.close()
                            self._worker = self._worker_socket(self._remoteAddress)

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

        self._close()

    def _sendHeartBeat(self):
        if (time.time() - self._lastSentTime > HEARTBEAT_INTERVAL and self._active is True):
            self._send(self._worker, signals.PPP_HEARTBEAT)

    def _checkReady(self):
        if(self._ready is True and self._active is True and self._readySent is False):
            self._readySent = True
            self._notReadySent = False
            self._send(self._worker, signals.PPP_READY)
        elif (self._ready is False and self._active is True and self._notReadySent is False):
            self._notReadySent = True
            self._readySent = False
            self._send(self._worker, signals.PPP_NOT_READY)

    def ready(self):
        self._ready = True

    def notReady(self):
        self._ready = False

    def onReady(self):
        if(self._onReady and self._active):
            self._onReady(self._remoteAddress)

    def onNotReady(self):
        if(self._onNotReady and self._active):
            self._onNotReady(self._remoteAddress)

    def close(self, force=True):
        if (self._active is False):
            log.warning("Attempting to close inactive ZMQListener")
        else:
            self._active = False
            self._closeForce = force

    def waitForClose(self):
        log.info("start closing connection for node {node} in address {address}", node=self._nodeName, address=self._remoteAddress)
        while(self._isClosed is False):
            time.sleep(1)
        log.info("finish closing connection for node {node} in address {address}", node=self._nodeName, address=self._remoteAddress)

    def _close(self):
        if (self._worker is None):
            log.warning("unable to close zmqListener, worker is none")
            return

        self._send(self._worker, signals.PPP_DISCONNECT)

        if (self._closeForce is False):
            try:
                time.sleep(2)
                readAfterStopped = 0
                result = self._worker.poll(STOP_TIMEOUT_MS)
                while result == zmq.POLLIN:
                    lock.acquire()
                    try:
                        frames = self._worker.recv_multipart()
                        signal = frames[0]
                        if (signal == signals.PPP_MSG):
                            result = self._handleAMessage(frames)
                            self._send(self._worker, signals.PPP_DONE_DISCONNECT, result)
                            readAfterStopped += 1
                            log.warning('read message during stop {readAfterStopped}', readAfterStopped=readAfterStopped)
                        else:
                            log.warning('read signal {signal} during stop', signal=signal)
                    finally:
                        lock.release()
                    result = self._worker.poll(STOP_TIMEOUT_MS)
            except Exception as e:
                log.error('Error on zmqListener close {e}', e=str(e))

        self._worker.close()
        self._isClosed = True
