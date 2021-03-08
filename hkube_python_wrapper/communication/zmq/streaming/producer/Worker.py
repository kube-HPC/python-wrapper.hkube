import time

EXPIRY = 30

class Worker(object):
    def __init__(self, address):
        self.address = address
        self.expiry = time.time() + EXPIRY
