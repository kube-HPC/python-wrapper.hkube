#
# Based on Paranoid Pirate queue
#
#   Author: Daniel Lundin <dln(at)eintr(dot)org>
#
import time
from .Flow import Flow
from .MessageQueue import MessageQueue
from hkube_python_wrapper.util.logger import log
from hkube_python_wrapper.communication.zmq.streaming import signals
import zmq

HEARTBEAT_LIVENESS = 5
CYCLE_LENGTH_MS = 1000

context = zmq.Context()


class ZMQProducer(object):
    def __init__(self, port, maxMemorySize, responseAccumulator, queueTimeAccumulator, consumerTypes, encoding, nodeName):
        self.nodeName = nodeName
        self.encoding = encoding
        self.responseAccumulator = responseAccumulator
        self.queueTimeAccumulator = queueTimeAccumulator
        self.maxMemorySize = maxMemorySize
        self.port = port
        self.consumerTypes = consumerTypes
        self.messageQueue = MessageQueue(consumerTypes, self.nodeName)
        self._backend = context.socket(zmq.ROUTER)  # ROUTER
        self._backend.bind("tcp://*:" + str(port))  # For workers
        log.info("Producer listening on tcp://*:{port}", port=port)
        self._active = True
        self._working = True
        self.watingForResponse = {}

    def produce(self, header, message, messageFlowPattern=[]):
        while (self.messageQueue.sizeSum > self.maxMemorySize):
            self.messageQueue.loseMessage()
        appendTime = time.time()
        self.messageQueue.append(messageFlowPattern, header, message, appendTime)

    def start(self):  # pylint: disable=too-many-branches
        poll_workers = zmq.Poller()
        poll_workers.register(self._backend, zmq.POLLIN)

        while self._active:  # pylint: disable=too-many-nested-blocks
            try:
                socks = dict(poll_workers.poll(CYCLE_LENGTH_MS))

                if socks.get(self._backend) == zmq.POLLIN:

                    frames = self._backend.recv_multipart() or []

                    if (len(frames) != 4):
                        log.warning("got {len} frames {frames}", len=len(frames), frames=frames)
                        continue

                    address, signal, consumer, result = frames  # pylint: disable=unbalanced-tuple-unpacking
                    consumerType = self.encoding.decode(value=consumer, plainEncode=True)

                    if (not consumerType in self.consumerTypes):
                        log.warning("Producer got message from unknown consumer: {consumerType}, dropping the message", consumerType=consumerType)
                        continue

                    if (signal == signals.PPP_DONE):
                        sentTime = self.watingForResponse.get(address)
                        if (sentTime):
                            now = time.time()
                            del self.watingForResponse[address]
                            self.responseAccumulator(result, consumerType, round((now - sentTime) * 1000, 4))
                        else:
                            log.error('missing from watingForResponse:' + str(signal))

                    elif (signal == signals.PPP_READY):
                        message = self.messageQueue.pop(consumerType)
                        if (message):
                            messageFlowPattern, header, payload, appendTime = message
                            flow = Flow(messageFlowPattern)
                            flowMsg = self.encoding.encode(flow.getRestOfFlow(self.nodeName), plainEncode=True)
                            frames = [address, signals.PPP_MSG, flowMsg, header, payload]
                            self.watingForResponse[address] = time.time()
                            queueTime = round((time.time() - appendTime) * 1000, 4)
                            self.queueTimeAccumulator(consumerType, queueTime)
                            self._send(frames)
                        else:
                            self._send([address, signals.PPP_NO_MSG])

            except Exception as e:
                log.error('Error in ZMQProducer {e}', e=str(e))

        self._working = False

    def _send(self, frames):
        self._backend.send_multipart(frames, copy=False)

    def queueSize(self, consumerSize):
        return self.messageQueue.size(consumerSize)

    def sent(self, consumerType):
        return self.messageQueue.sent[consumerType]

    def resetQueue(self):
        self.messageQueue.resetAll()

    def resetQueuePartial(self, numberOfMessagesToRemove):
        self.messageQueue.reset(numberOfMessagesToRemove)

    def close(self, force=True):
        log.info('queue size before close = {len}', len=len(self.messageQueue.queue))
        while self.messageQueue.queue and not force:
            log.info('queue size during close = {len}', len=len(self.messageQueue.queue))
            time.sleep(1)
        self._active = False
        while self._working:
            time.sleep(1)
        self._backend.close()
        log.info('queue size after close = {len}', len=len(self.messageQueue.queue))
