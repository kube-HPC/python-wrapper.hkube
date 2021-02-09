import time
import zmq
import uuid
import threading

HEARTBEAT_LIVENESS = 5
HEARTBEAT_INTERVAL = 1
INTERVAL_INIT = 1
INTERVAL_MAX = 32
#  Paranoid Pirate Protocol constants
PPP_READY = b"\x01"  # Signals worker is ready
PPP_HEARTBEAT = b"\x02"  # Signals worker heartbeat
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
        print("zmq listener connecting to " + remoteAddress)
        self.send(worker, [PPP_READY])
        return worker

    def send(self, workder, arr):
        arr.append(self.consumerType)
        workder.send_multipart(arr)

    def handleAMessage(self, frames):
        encodedMessageFlowPattern, header, message = frames  # pylint: disable=unbalanced-tuple-unpacking
        messageFlowPattern = self.encoding.decode(value=encodedMessageFlowPattern, plainEncode=True)
        return self.onMessage(messageFlowPattern, header, message)

    def start(self):  # pylint: disable=too-many-branches
        liveness = HEARTBEAT_LIVENESS
        interval = INTERVAL_INIT

        heartbeat_at = time.time() + HEARTBEAT_INTERVAL
        self.worker = self.worker_socket(self.remoteAddress)
        result = None
        while self.active:
            try:
                result = self.worker.poll(HEARTBEAT_INTERVAL * 1000)
            except Exception as e:
                if (self.active):
                    print('Error during poll of ' + str(self.remoteAddress) + ' ' + str(e))
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
                        print('Error during receive of ' + str(self.remoteAddress) + ' ' + str(e))
                        raise e
                    break
                if not frames:
                    if (self.active):
                        raise Exception("Connection to producer on " + self.remoteAddress + " interrupted")
                    break

                if len(frames) == 3:
                    liveness = HEARTBEAT_LIVENESS
                    result = self.handleAMessage(frames)
                    lock.release()
                    try:
                        self.send(self.worker, [result])
                    except Exception as e:
                        if (self.active):
                            print(e)
                            raise e
                        break
                elif len(frames) == 1 and frames[0] == PPP_HEARTBEAT:
                    liveness = HEARTBEAT_LIVENESS
                    lock.release()
                else:
                    print("E: Invalid message: %s" % frames)
                    liveness = HEARTBEAT_LIVENESS
                    lock.release()

                interval = INTERVAL_INIT
            else:
                liveness -= 1
                if liveness == 0:
                    print("W: Heartbeat failure, can't reach queue of " + str(self.remoteAddress))
                    print("W: Reconnecting in %0.2fs" % interval)
                    time.sleep(interval)

                    if interval < INTERVAL_MAX:
                        interval *= 2
                    try:
                        self.worker.setsockopt(zmq.LINGER, 0)
                        self.worker.close()
                        self.worker = None
                    except Exception as e:
                        if (self.active):
                            print('Error on heartbeat failure for ' + str(self.remoteAddress) + str(e))
                        else:
                            break
                    if (self.active):
                        self.worker = self.worker_socket(self.remoteAddress, self.context)
                    liveness = HEARTBEAT_LIVENESS

            if time.time() > heartbeat_at:
                heartbeat_at = time.time() + HEARTBEAT_INTERVAL
                try:
                    self.send(self.worker, [PPP_HEARTBEAT])
                except Exception as e:
                    if (self.active):
                        print(e)
                    else:
                        break

    def close(self):
        # pylint: disable=too-many-nested-blocks
        if not (self.active):
            print("Attempting to close inactive ZMQListener")
        else:
            self.active = False
            time.sleep(HEARTBEAT_LIVENESS + 1)
            if (self.worker is not None):
                readAfterStopped = 0
                try:
                    result = self.worker.poll(HEARTBEAT_INTERVAL * 1000)
                    while result == zmq.POLLIN:
                        frames = self.worker.recv_multipart()
                        if len(frames) == 3:
                            readAfterStopped += 1
                            print('Read after stop ' + str(readAfterStopped))
                        result = self.worker.poll(HEARTBEAT_INTERVAL * 1000)
                        while result == zmq.POLLIN:
                            lock.acquire()
                            frames = self.worker.recv_multipart()
                            if len(frames) == 3:
                                encodedMessageFlowPattern, header, message = frames  # pylint: disable=unbalanced-tuple-unpacking
                                messageFlowPattern = self.encoding.decode(value=encodedMessageFlowPattern, plainEncode=True)
                                self.onMessage(messageFlowPattern, header, message)
                                readAfterStopped += 1
                                print('Read after stop ' + str(readAfterStopped))
                            lock.release()
                            result = self.worker.poll(HEARTBEAT_INTERVAL * 1000)
                except Exception as e:
                    try:
                        lock.release()
                    finally:
                        print('Error on zmqListener close' + str(e))
                self.worker.close()
                self.context.destroy()
