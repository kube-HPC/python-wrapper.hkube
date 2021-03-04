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

        while self.active:  # pylint: disable=too-many-nested-blocks
            try:
                socks = dict(poll_workers.poll(CYCLE_LENGTH_MS))

                if socks.get(self._backend) == zmq.POLLIN:

                    frames = self._backend.recv_multipart()

                    if not frames:
                        raise Exception("Unexpected router no frames on receive, no address frame")

                    if(len(frames) != 4):
                        log.warning("got {len} frames {frames}", len=len(frames), frames=frames)
                        continue

                    address, signal, consumer, result = frames # pylint: disable=unbalanced-tuple-unpacking
                    consumerType = self.encoding.decode(value=consumer, plainEncode=True)

                    if (not consumerType in self.consumerTypes):
                        log.warning("Producer got message from unknown consumer: {consumerType}, dropping the message", consumerType=consumerType)
                        continue

                    if (signal in (PPP_NOT_READY, PPP_DISCONNECT)):
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

                    if time.time() >= heartbeat_at:
                        for consumerType, workersOfType in workers.queues.items():
                            for worker in workersOfType:
                                msg = [worker, PPP_HEARTBEAT]
                                self._send(msg)

                        heartbeat_at = time.time() + HEARTBEAT_INTERVAL

                for consumerType, workerQueue in workers.queues.items():
                    if (workerQueue):
                        message = self.messageQueue.pop(consumerType)
                        if (message):
                            messageFlowPattern, header, payload = message
                            flow = Flow(messageFlowPattern)
                            worker = workers.nextWorker(consumerType)
                            frames = [worker, PPP_MSG, self.encoding.encode(flow.getRestOfFlow(self.me), plainEncode=True), header, payload]
                            self.watingForResponse[worker] = time.time()
                            self._send(frames)

            except Exception as e:
                if (self.active):
                    log.error('Error in ZMQProducer {e}', e=str(e))
                else:
                    break
            workers.purge()

    def _send(self, frames):
        self._backend.send_multipart(frames, copy=False)

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
