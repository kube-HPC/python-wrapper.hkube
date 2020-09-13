from __future__ import print_function, division, absolute_import


class ZmqConsts(object):
    def __init__(self):
        self.ping = b"ping"
        self.pong = b"pong"


class Consts(object):
    def __init__(self):
        self.zmq = ZmqConsts()


consts = Consts()
