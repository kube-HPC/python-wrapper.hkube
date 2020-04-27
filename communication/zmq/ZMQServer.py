import zmq.green as zmq
from gevent import spawn
from zmq.utils.monitor import recv_monitor_message

from util.decorators import timing

context = zmq.Context()


class ZMQServer(object):
    def __init__(self):
        self._numberOfConn = 0
        self._active = True
        self._getReplyFunc = None
        self._socket = None

    def listen(self, port, getReplyFunc):
        self._getReplyFunc = getReplyFunc
        self._socket = context.socket(zmq.REP)
        self._socket.bind("tcp://*:" + str(port))

        class Object(object):
            pass

        socketMoniotr = self._socket.get_monitor_socket()

        def invokeOnEvent(monitor, onConnect, onDisconnect):
            while monitor.poll():
                evt = recv_monitor_message(monitor)
                if evt['event'] == zmq.EVENT_HANDSHAKE_SUCCEEDED:
                    onConnect()
                if evt['event'] == zmq.EVENT_DISCONNECTED:
                    onDisconnect()
        spawn(invokeOnEvent,socketMoniotr, self.onConnect, self.onDisconnect)
        while self._active:
            message = self._socket.recv()
            self.send(message)


    @timing
    def send(self, message):
        self._socket.send(self._getReplyFunc(message), copy=False)

    def onConnect(self):
        self._numberOfConn = +1

    def onDisconnect(self):
        self._numberOfConn = -1

    def isServing(self):
        return self._numberOfConn > 0

    def stop(self):
        self._active = False

    def close(self):
        self._socket.close()
