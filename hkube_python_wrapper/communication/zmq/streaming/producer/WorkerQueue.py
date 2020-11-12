from collections import OrderedDict
import time

class WorkerQueue(object):
    def __init__(self, consumerTypes):
        self.queues = {}
        for consumerType in consumerTypes:
            self.queues[consumerType] = OrderedDict()

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
            for (address, consumerType) in expired:
                print("W: Idle worker expired: %s" % address)
                self.queues[consumerType].pop(address, None)

    def next(self, type):
        address, worker = self.queues[type].popitem(False)  # pylint: disable=unused-variable
        return address
