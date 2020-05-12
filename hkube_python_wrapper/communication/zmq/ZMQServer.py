import zmq.green as zmq
from gevent import spawn
import gevent
from zmq.utils.monitor import recv_monitor_message
from hkube_python_wrapper.util.decorators import timing

context = zmq.Context()


class ZMQServer(object):
    def __init__(self):
        self._numberOfConn = 0
        self._active = True
        self._createReplyFunc = None
        self._socket = None

    def listen(self, port, getReplyFunc):
        self._createReplyFunc = getReplyFunc
        self._socket = context.socket(zmq.REP)
        self._socket.bind("tcp://*:" + str(port))

        socketMoniotr = self._socket.get_monitor_socket()

        def invokeOnEvent(monitor, onConnect, onDisconnect):
            while True:
                res = monitor.poll(0.1)
                if (res != 0):
                    evt = recv_monitor_message(monitor)
                    if evt['event'] == zmq.EVENT_HANDSHAKE_SUCCEEDED:
                        onConnect()
                    if evt['event'] == zmq.EVENT_DISCONNECTED:
                        onDisconnect()
                gevent.sleep(0.1)
        spawn(invokeOnEvent, socketMoniotr, self.onConnect, self.onDisconnect)

        def onRecieve():
            while self._active:
                message = self._socket.recv()
                self.send(message)
                print('sent back')
        spawn(onRecieve)

    @timing
    def send(self, message):
        toBeSent = self._createReplyFunc(message)
        self._socket.send(toBeSent, copy=False)

    def onConnect(self):
        self._numberOfConn += 1

    def onDisconnect(self):
        self._numberOfConn -= 1

    def isServing(self):
        return self._numberOfConn > 0

    def stop(self):
        self._active = False

    def close(self):
        self._socket.close()
