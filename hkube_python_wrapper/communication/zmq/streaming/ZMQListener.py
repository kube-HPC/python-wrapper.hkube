from random import randint
import time
import gevent
import zmq.green as zmq
import msgpack

HEARTBEAT_LIVENESS = 3
HEARTBEAT_INTERVAL = 1
INTERVAL_INIT = 1
INTERVAL_MAX = 32
#  Paranoid Pirate Protocol constants
PPP_READY = b"\x01"  # Signals worker is ready
PPP_HEARTBEAT = b"\x02"  # Signals worker heartbeat


class ZMQListener(object):

    def __init__(self, remoteAddress, onMessage, consumerType):
        self.onMessage = onMessage
        self.consumerType = consumerType
        self.remoteAddress = remoteAddress
        self.active = True
        self.worker = None

    def worker_socket(self, context, remoteAddress, poller):
        """Helper function that returns a new configured socket
           connected to the Paranoid Pirate queue"""
        worker = context.socket(zmq.DEALER)  # DEALER
        identity = ('id' + str(randint(0, 0x10000)) + '-' + str(randint(0, 0x10000))).encode()
        worker.setsockopt(zmq.IDENTITY, identity)
        poller.register(worker, zmq.POLLIN)
        worker.connect(remoteAddress)
        print("zmq listener connecting to " + remoteAddress)
        worker.send_multipart([PPP_READY, msgpack.packb(self.consumerType)])
        return worker

    def start(self): # pylint: disable=too-many-branches
        context = zmq.Context(1)
        poller = zmq.Poller()
        liveness = HEARTBEAT_LIVENESS
        interval = INTERVAL_INIT

        heartbeat_at = time.time() + HEARTBEAT_INTERVAL
        self.worker = self.worker_socket(context, self.remoteAddress, poller)
        cycles = 0
        while self.active:
            gevent.sleep()
            try:
                socks = dict(poller.poll(HEARTBEAT_INTERVAL * 1000))
            except Exception as e:
                if (self.active):
                    print(e)

            # Handle worker activity on backend
            if socks.get(self.worker) == zmq.POLLIN:
                #  Get message
                #  - 3-part envelope + content -> request
                #  - 1-part HEARTBEAT -> heartbeat
                try:
                    frames = self.worker.recv_multipart()
                except Exception as e:
                    if(self.active):
                        print(e)
                if not frames:
                    break  # Interrupted

                if len(frames) == 1 and not frames[0] == PPP_HEARTBEAT:
                    # Simulate various problems, after a few cycles
                    cycles += 1
                    if (cycles % 100 == 0):
                        decoded = str(frames[0])
                        print(decoded)
                        print(cycles)
                    liveness = HEARTBEAT_LIVENESS
                    result = self.onMessage(frames[0])
                    newFrames = [result, msgpack.packb(self.consumerType)]
                    try:
                        self.worker.send_multipart(newFrames)
                    except Exception as e:
                        if(self.active):
                            print(e)

                elif len(frames) == 1 and frames[0] == PPP_HEARTBEAT:
                    liveness = HEARTBEAT_LIVENESS
                else:
                    print("E: Invalid message: %s" % frames)
                    liveness = HEARTBEAT_LIVENESS

                interval = INTERVAL_INIT
            else:
                liveness -= 1
                if liveness == 0:
                    print("W: Heartbeat failure, can't reach queue")
                    print("W: Reconnecting in %0.2fs" % interval)
                    gevent.sleep(interval)

                    if interval < INTERVAL_MAX:
                        interval *= 2
                    poller.unregister(self.worker)
                    try:
                        self.worker.setsockopt(zmq.LINGER, 0)
                        self.worker.close()
                    except Exception as e:
                        if(self.active):
                            print(e)
                    self.worker = self.worker_socket(context, self.remoteAddress, poller)
                    liveness = HEARTBEAT_LIVENESS

            if time.time() > heartbeat_at:
                heartbeat_at = time.time() + HEARTBEAT_INTERVAL
                try:
                    self.worker.send_multipart([PPP_HEARTBEAT, msgpack.packb(self.consumerType)])
                except Exception as e:
                    if (self.active):
                        print(e)

    def close(self):
        if not (self.active):
            print("Attempting to close inactive ZMQListener")
        else:
            self.active = False
            self.worker.close()

# if __name__ == "__main__":
#     def doSomething():
#         gevent.sleep(3)
#
#
#     listener = ZMQListener('tcp://localhost:5556', doSomething)
#     gevent.spawn(listener.start)
#     queue = ZMQPublisher(port=5556, maxMemorySize=5000)
#     gevent.spawn(queue.start)
#     gevent.sleep(5)

# thread=Thread(target=listener.start)
# thread.start()
# client.start()
