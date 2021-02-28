from collections import OrderedDict
import time
from hkube_python_wrapper.util.logger import log


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
        for consumer, queue in self.queues.items():
            for address, worker in queue.items():
                if t > worker.expiry:  # Worker expired
                    expired.append((address, consumer))
            for (address, consumerType) in expired:
                log.warning("Idle worker expired: {address}", address=address)
                self.queues[consumerType].pop(address, None)

    def nextWorker(self, consumerType):
        address, _ = self.queues[consumerType].popitem(False)
        return address
