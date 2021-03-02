#
# Based on Paranoid Pirate queue
#
#   Author: Daniel Lundin <dln(at)eintr(dot)org>
#
import time
from .Flow import Flow
from .Worker import Worker
from .WorkerQueue import WorkerQueue
from .MessageQueue import MessageQueue
from hkube_python_wrapper.util.logger import log
from hkube_python_wrapper.communication.zmq.streaming.consts import *
import zmq

HEARTBEAT_INTERVAL = 5
HEARTBEAT_LIVENESS = 5
CYCLE_LENGTH_MS = 1
shouldPrint = False

class ZMQProducer(object):
    def __init__(self, port, maxMemorySize, responseAccumulator, consumerTypes, encoding, me):
        self.me = me
        self.encoding = encoding
        self.responseAccumulator = responseAccumulator
        self.maxMemorySize = maxMemorySize
        self.port = port
        self.consumerTypes = consumerTypes
        self.messageQueue = MessageQueue(consumerTypes, self.me)
        context = zmq.Context()
        self._backend = context.socket(zmq.ROUTER)  # ROUTER
        self._backend.bind("tcp://*:" + str(port))  # For workers
        log.info("Producer listening on tcp://*:{port}", port=port)
        self.active = True
        self.watingForResponse = {}

    def produce(self, header, message, messageFlowPattern=[]):
        while (self.messageQueue.sizeSum > self.maxMemorySize):
            self.messageQueue.loseMessage()
        self.messageQueue.append(messageFlowPattern, header, message)

    def start(self):  # pylint: disable=too-many-branches
        poll_workers = zmq.Poller()
        poll_workers.register(self._backend, zmq.POLLIN)
        workers = WorkerQueue(self.consumerTypes)
        heartbeat_at = time.time() + HEARTBEAT_INTERVAL

        while self.active:
            try:
                socks = dict(poll_workers.poll(CYCLE_LENGTH_MS))
            except Exception as e:
                if (self.active):
                    log.error(e)
                else:
                    break
            # Handle worker activity on self._backend
            if socks.get(self._backend) == zmq.POLLIN:
                # Use worker address for LRU routing
                try:
                    frames = self._backend.recv_multipart()
                except Exception as e:
                    if (self.active):
                        log.error(e)
                    else:
                        break
                if not frames:
                    if (self.active):
                        raise Exception("Unexpected router no frames on receive, no address frame")
                    break

                if(len(frames) > 4):
                    log.warning("got {frames} frames ", frames=len(frames))
                    continue

                address, signal, consumer, result = frames # pylint: disable=unbalanced-tuple-unpacking
                consumerType = self.encoding.decode(value=consumer, plainEncode=True)

                signalStr = signals.get(signal)
                if(signalStr and shouldPrint):
                    print('---- got {signal} from {address} ----'.format(signal=signalStr, address=address))

                if (not consumerType in self.consumerTypes):
                    log.warning("Producer got message from unknown consumer: {consumerType}, dropping the message", consumerType=consumerType)
                    continue

                if (signal == PPP_NOT_READY):
                    workers.notReady(consumerType, address)
                    continue

                if (signal == PPP_DONE):
                    sentTime = self.watingForResponse.get(address)
                    if (sentTime):
                        now = time.time()
                        del self.watingForResponse[address]
                        self.responseAccumulator(result, consumerType, round((now - sentTime) * 1000, 4))
                    else:
                        log.error('missing from watingForResponse:' + str(signal))

                if (not address in self.watingForResponse.keys() and signal in (PPP_INIT, PPP_READY, PPP_HEARTBEAT, PPP_DONE)):
                    expiry = time.time() + (HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS)
                    workers.ready(Worker(address, expiry), consumerType)

                # Validate control message, or return reply to client
                if time.time() >= heartbeat_at:
                    for consumerType, workersOfType in workers.queues.items():
                        for worker in workersOfType:
                            msg = [worker, PPP_HEARTBEAT]
                            try:
                                self._backend.send_multipart(msg, copy=False)
                            except Exception as e:
                                if (self.active):
                                    log.error(e)
                                else:
                                    break
                    heartbeat_at = time.time() + HEARTBEAT_INTERVAL
            for consumerType, workerQueue in workers.queues.items():
                if (workerQueue):
                    poped = self.messageQueue.pop(consumerType)
                    if (poped):
                        messageFlowPattern, header, payload = poped
                        flow = Flow(messageFlowPattern)
                        identity = workers.nextWorker(consumerType)
                        frames = [identity, self.encoding.encode(flow.getRestOfFlow(self.me), plainEncode=True), header, payload]
                        self.watingForResponse[identity] = time.time()
                        try:
                            print('sending message to worker {identity}'.format(identity=identity))
                            self._backend.send_multipart(frames, copy=False)
                        except Exception as e:
                            if (self.active):
                                log.error(e)
                            else:
                                break
            workers.purge()

    def queueSize(self, consumerSize):
        return self.messageQueue.size(consumerSize)

    def sent(self, consumerType):
        return self.messageQueue.sent[consumerType]

    def close(self, force=True):
        log.info('queue size during close = ' + str(len(self.messageQueue.queue)))
        while self.messageQueue.queue and not force:
            time.sleep(1)
        log.info('queue empty, closing producer')
        self.active = False
        time.sleep(HEARTBEAT_LIVENESS + 1)
        if not force:
            self._backend.close(10)
        else:
            self._backend.close(0)
