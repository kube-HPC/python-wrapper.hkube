#
# Paranoid Pirate queue
#
#   Author: Daniel Lundin <dln(at)eintr(dot)org>
#
import time
from .Flow import Flow
from .Worker import Worker
from .WorkerQueue import WorkerQueue
from .MessageQueue import MessageQueue
import zmq

HEARTBEAT_LIVENESS = 5  # 3..5 is reasonable
HEARTBEAT_INTERVAL = 1.0  # Seconds

#  Paranoid Pirate Protocol constants
PPP_READY = b"\x01"  # Signals worker is ready
PPP_HEARTBEAT = b"\x02"  # Signals worker heartbeat


class ZMQProducer(object):
    def __init__(self, port, maxMemorySize, responseAcumulator, consumerTypes, encoding, me):
        self.me = me
        self.encoding = encoding
        self.responseAcumulator = responseAcumulator
        self.maxMemorySize = maxMemorySize
        self.port = port
        self.consumerTypes = consumerTypes
        self.messageQueue = MessageQueue(consumerTypes, self.me)
        context = zmq.Context(1)
        self._backend = context.socket(zmq.ROUTER)  # ROUTER
        self._backend.bind("tcp://*:" + str(port))  # For workers
        print("Producer listening on " + "tcp://*:" + str(port))
        self.active = True

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
            poller = poll_workers
            try:
                socks = dict(poller.poll(HEARTBEAT_INTERVAL * 1000))
            except Exception as e:
                if (self.active):
                    print(e)
                else:
                    break
            # Handle worker activity on self._backend
            if socks.get(self._backend) == zmq.POLLIN:
                # Use worker address for LRU routing
                try:
                    frames = self._backend.recv_multipart()
                except Exception as e:
                    if (self.active):
                        print(e)
                    else:
                        break
                if not frames:
                    if (self.active):
                        raise Exception("Unexpected router no frames on receive, no address frame")
                    break
                address = frames[0]
                consumerType = self.encoding.decode(value=frames[2], plainEncode=True)
                if not consumerType in self.consumerTypes:
                    print("Producer got message from unknown consumer: " + consumerType + ", dropping the message")
                    continue
                if frames[1] not in (PPP_READY, PPP_HEARTBEAT):
                    self.responseAcumulator(frames[1], consumerType)
                workers.ready(Worker(address), consumerType)

                # Validate control message, or return reply to client
                msg = frames[1:]
                if time.time() >= heartbeat_at:
                    for consumerType, workersOfType in workers.queues.items():
                        for worker in workersOfType:
                            msg = [worker, PPP_HEARTBEAT]
                            try:
                                self._backend.send_multipart(msg)
                            except Exception as e:
                                if (self.active):
                                    print(e)
                                else:
                                    break
                    heartbeat_at = time.time() + HEARTBEAT_INTERVAL
            for consumerType, workerQueue in workers.queues.items():
                if (workerQueue):
                    poped = self.messageQueue.pop(consumerType)
                    if (poped):
                        messageFlowPattern, header, payload = poped
                        flow = Flow(messageFlowPattern)
                        frames = [self.encoding.encode(flow.getRestOfFlow(self.me), plainEncode=True), header, payload]

                        frames.insert(0, workers.next(consumerType))
                        try:
                            self._backend.send_multipart(frames)
                        except Exception as e:
                            if (self.active):
                                print(e)
                            else:
                                break
            workers.purge()

    def queueSize(self, consumerSize):
        return self.messageQueue.size(consumerSize)

    def sent(self, consumerType):
        return self.messageQueue.sent[consumerType]

    def close(self, force=True):
        while not self.messageQueue.queue and not force:
            time.sleep(1)
        self.active = False
        self._backend.close()
