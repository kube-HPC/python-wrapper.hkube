import time

HEARTBEAT_LIVENESS = 3  # 3..5 is reasonable
HEARTBEAT_INTERVAL = 1.0  # Seconds


class Worker(object):
    def __init__(self, address):
        self.address = address
        self.expiry = time.time() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS
