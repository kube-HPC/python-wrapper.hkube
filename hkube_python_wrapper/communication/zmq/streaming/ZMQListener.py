import time
import zmq
import uuid
import threading
from hkube_python_wrapper.util.logger import log

CYCLE_LENGTH_MS = 1000
HEARTBEAT_INTERVAL = 5
HEARTBEAT_LIVENESS = 5
INTERVAL_INIT = 1
INTERVAL_MAX = 32


#  Paranoid Pirate Protocol constants
PPP_READY = b"\x01"  # Signals worker is ready
PPP_HEARTBEAT = b"\x02"  # Signals worker heartbeat
PPP_DISCONNECT = b"\x03"  # Disconnect
lock = threading.Lock()


class ZMQListener(object):

    def __init__(self, remoteAddress, onMessage, encoding, consumerType):
        self.encoding = encoding
        self.onMessage = onMessage
        self.consumerType = self.encoding.encode(consumerType, plainEncode=True)
        self.remoteAddress = remoteAddress
        self.active = True
        self.worker = None
        self.context = None
        self.heartbeat_at = None

    def worker_socket(self, remoteAddress, context=None):
        """Helper function that returns a new configured socket
           connected to the Paranoid Pirate queue"""
        if not context:
            context = zmq.Context(1)
        self.context = context
        worker = self.context.socket(zmq.DEALER)  # DEALER
        worker.setsockopt(zmq.LINGER, 0)
        identity = str(uuid.uuid4()).encode()
        worker.setsockopt(zmq.IDENTITY, identity)
        worker.setsockopt(zmq.LINGER, 0)
        worker.connect(remoteAddress)
        log.info("zmq listener connecting to {addr}", addr=remoteAddress)
        self.send(worker, [PPP_READY])
        return worker

    def send(self, workder, arr):
        arr.append(self.consumerType)
        workder.send_multipart(arr, copy=False)

    def handleAMessage(self, frames):
        encodedMessageFlowPattern, header, message = frames  # pylint: disable=unbalanced-tuple-unpacking
        messageFlowPattern = self.encoding.decode(value=encodedMessageFlowPattern, plainEncode=True)
        return self.onMessage(messageFlowPattern, header, message)

    def start(self):  # pylint: disable=too-many-branches
        liveness = HEARTBEAT_LIVENESS
        interval = INTERVAL_INIT

        self.heartbeat_at = time.time() + HEARTBEAT_INTERVAL
        self.worker = self.worker_socket(self.remoteAddress)
        result = None
        while self.active:
            try:
                result = self.worker.poll(CYCLE_LENGTH_MS)
            except Exception as e:
                if (self.active):
                    log.error('Error during poll of {addr}, {e}', addr=str(self.remoteAddress), e=str(e))
                    raise e
                break
            # Handle worker activity on backend
            if result == zmq.POLLIN:
                #  Get message
                #  - 3-part envelope + content -> request
                #  - 1-part HEARTBEAT -> heartbeat
                lock.acquire()
                try:
                    frames = self.worker.recv_multipart()
                except Exception as e:
                    lock.release()
                    if (self.active):
                        log.error('Error during receive of {addr}, {e}', addr=str(self.remoteAddress), e=str(e))
                        raise e
                    break
                if not frames:
                    lock.release()
                    if (self.active):
                        raise Exception("Connection to producer on " + self.remoteAddress + " interrupted")
                    break

                if len(frames) == 3:
                    liveness = HEARTBEAT_LIVENESS
                    try:
                        result = self.handleAMessage(frames)
                    finally:
                        lock.release()
                    try:
                        self.send(self.worker, [result])
                    except Exception as e:
                        if (self.active):
                            log.error(e)
                            raise e
                        break
                else:
                    lock.release()
                    if len(frames) == 1 and frames[0] == PPP_HEARTBEAT:
                        liveness = HEARTBEAT_LIVENESS
                    else:
                        log.error("Invalid message: {message}", message=frames)
                        liveness = HEARTBEAT_LIVENESS
                    try:
                        self.sendHeartBeat()
                    except Exception as e:
                        if (self.active):
                            log.error(e)
                        else:
                            break

                interval = INTERVAL_INIT
            else:
                liveness -= 1
                if liveness == 0:
                    log.warning("Heartbeat failure, can't reach queue of {addr}", addr=str(self.remoteAddress))
                    log.warning("Reconnecting in {interval:0.2f}", interval=interval)
                    time.sleep(interval)

                    if interval < INTERVAL_MAX:
                        interval *= 2
                    try:
                        if (self.active):
                            self.worker.close()
                            self.worker = None
                    except Exception as e:
                        if (self.active):
                            log.error('Error on heartbeat failure for {addr} {e}', addr=str(self.remoteAddress), e=str(e))
                        else:
                            break
                    if (self.active):
                        self.worker = self.worker_socket(self.remoteAddress, self.context)
                    liveness = HEARTBEAT_LIVENESS

                try:
                    self.sendHeartBeat()
                except Exception as e:
                    if (self.active):
                        log.error(e)
                    else:
                        break

    def sendHeartBeat(self):
        if time.time() > self.heartbeat_at:
            self.heartbeat_at = time.time() + HEARTBEAT_INTERVAL
            self.send(self.worker, [PPP_HEARTBEAT])

    def close(self, force=True):
        # pylint: disable=too-many-nested-blocks
        if not (self.active):
            log.warning("Attempting to close inactive ZMQListener")
        else:
            self.active = False
            if (self.worker is not None):
                if not (force):
                    time.sleep(HEARTBEAT_LIVENESS + 1)
                    readAfterStopped = 0
                    try:
                        result = self.worker.poll(HEARTBEAT_INTERVAL * 1000)
                        while result == zmq.POLLIN:
                            lock.acquire()
                            try:
                                frames = self.worker.recv_multipart()
                                if len(frames) == 3:
                                    self.handleAMessage(frames)
                                    readAfterStopped += 1
                                    log.warning('Read after stop {readAfterStopped}', readAfterStopped=readAfterStopped)
                                    self.send(self.worker, [PPP_DISCONNECT])
                                    break
                            finally:
                                lock.release()
                            result = self.worker.poll(HEARTBEAT_INTERVAL * 1000)
                    except Exception as e:
                        log.error('Error on zmqListener close {e}', e=str(e))
                else:
                    try:
                        self.send(self.worker, [PPP_DISCONNECT])
                    except Exception as e:
                        log.error('Error on sending disconnect {e}', e=str(e))
                self.worker.close()
                self.context.destroy()
