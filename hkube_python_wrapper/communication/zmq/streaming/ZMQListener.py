import time
import zmq
import uuid
import threading
from hkube_python_wrapper.util.logger import log
from hkube_python_wrapper.communication.zmq.streaming.consts import *

CYCLE_LENGTH_MS = 1000
HEARTBEAT_INTERVAL = 5
HEARTBEAT_LIVENESS = HEARTBEAT_INTERVAL * HEARTBEAT_INTERVAL
INTERVAL_INIT = 1
INTERVAL_MAX = 32

lock = threading.Lock()

shouldPrint = False

class ZMQListener(object):

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
        self._heartbeat_at = None
        self._ready = None
        self._readySent = None
        self._notReadySent = None

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
        self._send(worker, PPP_INIT)
        return worker

    def _send(self, worker, signal, result=PPP_EMPTY):
        if(worker):
            arr = [signal, self._consumerType, result]
            signalStr = signals.get(signal)
            if(signalStr and shouldPrint):
                print('---- sending {signal} from {address} ----'.format(signal=signalStr, address=self._remoteAddress))
            worker.send_multipart(arr, copy=False)

    def _handleAMessage(self, frames):
        encodedMessageFlowPattern, header, message = frames  # pylint: disable=unbalanced-tuple-unpacking
        messageFlowPattern = self._encoding.decode(value=encodedMessageFlowPattern, plainEncode=True)
        return self._onMessage(messageFlowPattern, header, message)

    def start(self):  # pylint: disable=too-many-branches
        liveness = HEARTBEAT_LIVENESS
        interval = INTERVAL_INIT

        self._heartbeat_at = time.time() + HEARTBEAT_INTERVAL
        self._worker = self._worker_socket(self._remoteAddress)
        result = None

        while self._active: # pylint: disable=too-many-nested-blocks
            try:
                result = self._worker.poll(CYCLE_LENGTH_MS)
                lock.acquire()

                if result == zmq.POLLIN:
                    frames = self._worker.recv_multipart()
                    if not frames:
                        raise Exception("Connection to producer on " + self._remoteAddress + " interrupted")

                    if len(frames) == 3:
                        liveness = HEARTBEAT_LIVENESS
                        self.onNotReady()
                        result = self._handleAMessage(frames)
                        self.onReady()
                        self._send(self._worker, PPP_DONE, result)
                    else:
                        if len(frames) == 1 and frames[0] == PPP_HEARTBEAT:
                            liveness = HEARTBEAT_LIVENESS
                        else:
                            log.error("Invalid message: {message}", message=frames)
                            liveness = HEARTBEAT_LIVENESS
                        self._sendHeartBeat()
                    interval = INTERVAL_INIT
                else:
                    liveness -= 1
                    if liveness == 0:
                        log.warning("Heartbeat failure {addr}, Reconnecting in {interval:0.2f}", addr=str(self._remoteAddress), interval=interval)
                        time.sleep(interval)

                        if interval < INTERVAL_MAX:
                            interval *= 2

                        if (self._active):
                            self._worker.close()
                            self._worker = None

                        if (self._active):
                            self._worker = self._worker_socket(self._remoteAddress, self._context)

                        liveness = HEARTBEAT_LIVENESS
                        self.sendHeartBeat()

            except Exception as e:
                if (self._active):
                    log.error('Error during poll of {addr}, {e}', addr=str(self._remoteAddress), e=str(e))
                    raise e
                break

            finally:
                lock.release()

    def _sendHeartBeat(self):
        if time.time() > self._heartbeat_at and self._ready is True:
            self._heartbeat_at = time.time() + HEARTBEAT_INTERVAL
            self._send(self._worker, PPP_HEARTBEAT)

    def ready(self):
        self._ready = True
        if(self._ready is True and not self._readySent):
            self._readySent = True
            self._notReadySent = False
            self._send(self._worker, PPP_READY)

    def notReady(self):
        self._ready = False
        if (self._ready is False and not self._notReadySent):
            self._notReadySent = True
            self._readySent = False
            self._send(self._worker, PPP_NOT_READY)

    def onReady(self):
        if(self._onReady):
            self._onReady(self._remoteAddress)

    def onNotReady(self):
        if(self._onNotReady):
            self._onNotReady(self._remoteAddress)

    def close(self, force=True):
        # pylint: disable=too-many-nested-blocks
        if not (self._active):
            log.warning("Attempting to close inactive ZMQListener")
        else:
            self._active = False
            if (self._worker is not None):
                if not (force):
                    time.sleep(HEARTBEAT_INTERVAL + 1)
                    readAfterStopped = 0
                    try:
                        result = self._worker.poll(HEARTBEAT_INTERVAL * 1000)
                        while result == zmq.POLLIN:
                            lock.acquire()
                            try:
                                frames = self._worker.recv_multipart()
                                if len(frames) == 3:
                                    self._handleAMessage(frames)
                                    readAfterStopped += 1
                                    log.warning('Read after stop {readAfterStopped}', readAfterStopped=readAfterStopped)
                                    self._send(self._worker, PPP_DISCONNECT)
                                    break
                            finally:
                                lock.release()
                            result = self._worker.poll(HEARTBEAT_INTERVAL * 1000)
                    except Exception as e:
                        log.error('Error on zmqListener close {e}', e=str(e))
                else:
                    try:
                        self._send(self._worker, PPP_DISCONNECT)
                    except Exception as e:
                        log.error('Error on sending disconnect {e}', e=str(e))
                self._worker.close()
                self._context.destroy()
