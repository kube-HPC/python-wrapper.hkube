#
##  Paranoid Pirate queue
#
#   Author: Daniel Lundin <dln(at)eintr(dot)org>
#

from collections import OrderedDict
import time

import zmq

HEARTBEAT_LIVENESS = 5  # 3..5 is reasonable
HEARTBEAT_INTERVAL = 1.0  # Seconds

#  Paranoid Pirate Protocol constants
PPP_READY = b"\x01"  # Signals worker is ready
PPP_HEARTBEAT = b"\x02"  # Signals worker heartbeat
RESPONSE_CACHE = 2000


class Worker(object):
    def __init__(self, address):
        self.address = address
        self.expiry = time.time() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS


class WorkerQueue(object):
    def __init__(self):
        self.queue = OrderedDict()

    def ready(self, worker):
        self.queue.pop(worker.address, None)
        self.queue[worker.address] = worker

    def purge(self):
        """Look for & kill expired workers."""
        t = time.time()
        expired = []
        for address, worker in self.queue.items():
            if t > worker.expiry:  # Worker expired
                expired.append(address)
        for address in expired:
            print("W: Idle worker expired: %s" % address)
            self.queue.pop(address, None)

    def next(self):
        address, worker = self.queue.popitem(False)  # pylint: disable=unused-variable
        return address


class MessageQueue(object):
    def __init__(self):
        self.sizeSum = 0
        self.queue = []

    def pop(self):
        out = self.queue.pop(0)
        self.sizeSum -= len(out)
        return out

    def append(self, msg):
        self.sizeSum -= len(msg)
        return self.queue.append(msg)

    def sumByteSize(self):
        return self.sizeSum


class ZMQPublisher(object):
    def __init__(self, port, maxMemorySize):
        self.maxMemorySize = maxMemorySize
        self.port = port
        self.messageQueue = MessageQueue()
        context = zmq.Context(1)
        self._backend = context.socket(zmq.ROUTER)  # ROUTER
        self._backend.bind("tcp://*:" + str(port))  # For workers
        self.active = True

    def send(self, message):
        while (self.messageQueue.sizeSum > self.maxMemorySize):
            time.sleep(0.1)
        self.messageQueue.append(message)

    def start(self):

        poll_workers = zmq.Poller()
        poll_workers.register(self._backend, zmq.POLLIN)
        workers = WorkerQueue()

        heartbeat_at = time.time() + HEARTBEAT_INTERVAL
        responses = []
        while self.active:
            poller = poll_workers
            socks = dict(poller.poll(HEARTBEAT_INTERVAL * 500))

            # Handle worker activity on self._backend
            if socks.get(self._backend) == zmq.POLLIN:
                # Use worker address for LRU routing
                frames = self._backend.recv_multipart()
                if not frames:
                    break
                if frames[1] not in (PPP_READY, PPP_HEARTBEAT):
                    responses.append(float(str(frames[1])))
                    if (responses > RESPONSE_CACHE):
                        responses.pop(0)

                address = frames[0]
                workers.ready(Worker(address))

                # Validate control message, or return reply to client
                msg = frames[1:]
                if time.time() >= heartbeat_at:
                    for worker in workers.queue:
                        msg = [worker, PPP_HEARTBEAT]
                        self._backend.send_multipart(msg)
                    heartbeat_at = time.time() + HEARTBEAT_INTERVAL
            if (workers.queue and self.messageQueue.queue):
                if (len(self.messageQueue.queue) % 100 == 0):
                    print(str(len(self.messageQueue.queue)))
                frames = [self.messageQueue.pop().encode()]
                frames.insert(0, workers.next())
                self._backend.send_multipart(frames)

            workers.purge()

    def close(self):
        self.active = False
        self._backend.close()
# if __name__ == "__main__":
#     queue = ZMQPublisher(port=5556, maxMemorySize=5000)
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
