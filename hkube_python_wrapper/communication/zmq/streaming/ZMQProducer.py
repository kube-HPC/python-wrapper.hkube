#
# Paranoid Pirate queue
#
#   Author: Daniel Lundin <dln(at)eintr(dot)org>
#

import time
from collections import OrderedDict

import gevent
import zmq.green as zmq
import msgpack

HEARTBEAT_LIVENESS = 5  # 3..5 is reasonable
HEARTBEAT_INTERVAL = 1.0  # Seconds

#  Paranoid Pirate Protocol constants
PPP_READY = b"\x01"  # Signals worker is ready
PPP_HEARTBEAT = b"\x02"  # Signals worker heartbeat


class Worker(object):
    def __init__(self, address):
        self.address = address
        self.expiry = time.time() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS


class WorkerQueue(object):
    def __init__(self, consumerTypes):
        self.queues = {}
        for consumberType in consumerTypes:
            self.queues[consumberType] = OrderedDict()

    def ready(self, worker, consumerType):
        self.queues[consumerType].pop(worker.address, None)
        self.queues[consumerType][worker.address] = worker

    def purge(self):
        """Look for & kill expired workers."""
        t = time.time()
        expired = []
        for type, queue in self.queues.items():
            for address, worker in queue.items():
                if t > worker.expiry:  # Worker expired
                    expired.append((address, type))
            for (address, cunsumerType) in expired:
                print("W: Idle worker expired: %s" % address)
                self.queues[cunsumerType].pop(address, None)

    def next(self, type):
        address, worker = self.queues[type].popitem(False)  # pylint: disable=unused-variable
        return address


class MessageQueue(object):
    def __init__(self, consumerTypes):
        self.consumerTypes = consumerTypes
        self.indexPerConsumer = OrderedDict()
        for consumerType in consumerTypes:
            self.indexPerConsumer[consumerType] = 0
        self.sizeSum = 0
        self.sentToAll = 0
        self.queue = []

    def hasItems(self, consumerType):
        return self.indexPerConsumer[consumerType] < len(self.queue)

    def pop(self, consumerType):
        index = self.indexPerConsumer[consumerType]
        out = self.queue[index]
        index += 1
        self.indexPerConsumer[consumerType] = index
        if (index == 1):
            anyZero = False
            for value in self.indexPerConsumer.values():
                if (value == 0):
                    anyZero = True
                    break
            if not (anyZero):
                self.queue.pop(0)
                self.sentToAll += 1
                self.sizeSum -= len(out)
                for key in self.indexPerConsumer.keys():
                    self.indexPerConsumer[key] = self.indexPerConsumer[key] - 1
        return out

    def append(self, msg):
        self.sizeSum -= len(msg)
        return self.queue.append(msg)

    def size(self, consumerType):
        index = self.indexPerConsumer[consumerType]
        size = len(self.queue) - index
        return size

    def sent(self, consumerType):
        index = self.indexPerConsumer[consumerType]
        sent = self.sentToAll + index
        return sent


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

    def produce(self, message):
        while (self.messageQueue.sizeSum > self.maxMemorySize):
            gevent.sleep(0.1)
        self.messageQueue.append(message)

    def start(self):
        poll_workers = zmq.Poller()
        poll_workers.register(self._backend, zmq.POLLIN)
        workers = WorkerQueue(self.consumerTypes)

        heartbeat_at = time.time() + HEARTBEAT_INTERVAL
        while self.active:
            gevent.sleep()
            poller = poll_workers
            socks = dict(poller.poll(HEARTBEAT_INTERVAL * 1000))
            # Handle worker activity on self._backend
            if socks.get(self._backend) == zmq.POLLIN:
                # Use worker address for LRU routing
                frames = self._backend.recv_multipart()
                if not frames:
                    break
                address = frames[0]
                consumerType = msgpack.unpackb(frames[2])
                if not consumerType in self.consumerTypes:
                    print("Producer got message from unknown consumer: " + consumerType + ", dropping the message")
                    continue
                print("Producer got message from " + consumerType)
                if frames[1] not in (PPP_READY, PPP_HEARTBEAT):
                    self.responseAcumulator(frames[1], consumerType)
                workers.ready(Worker(address), consumerType)

                # Validate control message, or return reply to client
                msg = frames[1:]
                if time.time() >= heartbeat_at:
                    for type, workersOfType in workers.queues.items():
                        for worker in workersOfType:
                            msg = [worker, PPP_HEARTBEAT]
                            self._backend.send_multipart(msg)
                    heartbeat_at = time.time() + HEARTBEAT_INTERVAL
            for type, workerQueu in workers.queues.items():
                if (workerQueu and self.messageQueue.hasItems(type)):
                    if (len(self.messageQueue.queue) % 100 == 0):
                        print(str(len(self.messageQueue.queue)))
                    frames = [self.messageQueue.pop(type)]
                    frames.insert(0, workers.next(type))
                    self._backend.send_multipart(frames)
            workers.purge()

    def queueSize(self, consumerSize):
        return self.messageQueue.size(consumerSize)

    def sent(self, consumerSize):
        return self.messageQueue.sent(consumerSize)

    def close(self):
        self.active = False
        self._backend.close()

# if __name__ == "__main__":
#     queue = MessageQueue(['a', 'b', 'c'])
#     queue.append('1')
#     queue.append('2')
#     queue.append('3')
#     item = queue.pop('a')
#     assert item == '1'
#     assert len(queue.queue) == 3
#     item = queue.pop('b')
#     assert item == '1'
#     assert len(queue.queue) == 3
#     item = queue.pop('a')
#     print(item)
#     assert item == '2'
#
#     item = queue.pop('c')
#     assert item == '1'
#     assert len(queue.queue) == 2

#     queue = ZMQPublisher(port=5556, maxMemorySize=5000)
#     gevent.spawn(queue.start)
#     gevent.sleep(3000)
#     thread = Thread(target=queue.start)
#     thread.start()
#     i = -1
#     while (i < 700):
#         i += 1
#         y = 0
#         while (y < 700):
#             y += 1
#             queue.send(str(i * 700 + y) + 'jjjj')
#         time.sleep(3)
