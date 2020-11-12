#
# Paranoid Pirate queue
#
#   Author: Daniel Lundin <dln(at)eintr(dot)org>
#
import time
from .Worker import Worker
from .WorkerQueue import WorkerQueue
from .MessageQueue import MessageQueue
import zmq
import msgpack

HEARTBEAT_LIVENESS = 5  # 3..5 is reasonable
HEARTBEAT_INTERVAL = 1.0  # Seconds

#  Paranoid Pirate Protocol constants
PPP_READY = b"\x01"  # Signals worker is ready
PPP_HEARTBEAT = b"\x02"  # Signals worker heartbeat


class ZMQProducer(object):
    def __init__(self, port, maxMemorySize, responseAcumulator, consumerTypes):
        self.responseAcumulator = responseAcumulator
        self.maxMemorySize = maxMemorySize
        self.port = port
        self.messageQueue = MessageQueue(consumerTypes)
        self.consumerTypes = consumerTypes
        context = zmq.Context(1)
        self._backend = context.socket(zmq.ROUTER)  # ROUTER
        self._backend.bind("tcp://*:" + str(port))  # For workers
        print("Producer listening on " + "tcp://*:" + str(port))
        self.active = True

    def produce(self, header, message):
        while (self.messageQueue.sizeSum > self.maxMemorySize):
            print('Loosing a message, queue filled up ')
            self.messageQueue.loseMessage()
        self.messageQueue.append(header, message)

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
            # Handle worker activity on self._backend
            if socks.get(self._backend) == zmq.POLLIN:
                # Use worker address for LRU routing
                try:
                    frames = self._backend.recv_multipart()
                except Exception as e:
                    if (self.active):
                        print(e)
                if not frames:
                    if (self.active):
                        raise Exception("Unexpected router no frames on receive, no address frame")
                address = frames[0]
                consumerType = msgpack.unpackb(frames[2])
                if not consumerType in self.consumerTypes:
                    print("Producer got message from unknown consumer: " + consumerType + ", dropping the message")
                    continue
                if frames[1] not in (PPP_READY, PPP_HEARTBEAT):
                    self.responseAcumulator(frames[1], consumerType)
                workers.ready(Worker(address), consumerType)

                # Validate control message, or return reply to client
                msg = frames[1:]
                if time.time() >= heartbeat_at:
                    for type, workersOfType in workers.queues.items():
                        for worker in workersOfType:
                            msg = [worker, PPP_HEARTBEAT]
                            try:
                                self._backend.send_multipart(msg)
                            except Exception as e:
                                if (self.active):
                                    print(e)
                    heartbeat_at = time.time() + HEARTBEAT_INTERVAL
            for type, workerQueu in workers.queues.items():
                if (workerQueu and self.messageQueue.hasItems(type)):
                    header, payload = self.messageQueue.pop(type)
                    frames = [header, payload]
                    frames.insert(0, workers.next(type))
                    try:
                        self._backend.send_multipart(frames)
                    except Exception as e:
                        if (self.active):
                            print(e)
            workers.purge()

    def queueSize(self, consumerSize):
        return self.messageQueue.size(consumerSize)

    def sent(self, consumerSize):
        return self.messageQueue.sent(consumerSize)

    def close(self):
        self.active = False
        self._backend.close()
