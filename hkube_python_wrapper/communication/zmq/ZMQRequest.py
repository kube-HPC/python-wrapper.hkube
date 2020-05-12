import gevent
import zmq
from gevent import spawn
from zmq.utils.monitor import recv_monitor_message
from hkube_python_wrapper.util.decorators import timing

context = zmq.Context()


class ZMQRequest(object):
    def __init__(self, reqDetails):
        self.socket = context.socket(zmq.REQ)
        self.socket.connect('tcp://' + reqDetails['host'] + ':' + str(reqDetails['port']))
        self.connected = False
        self.content = reqDetails['content']
        self.timeout = int(reqDetails['timeout']) * 1000
        self.pollTimeout = (self.timeout / 3)
        self.disconnected = False
        socketMonitor = self.socket.get_monitor_socket()

        def invokeOnEvent(monitor, onDisconnect):
            while not self.disconnected:
                if(monitor.poll(0.1)):
                    evt = recv_monitor_message(monitor)
                    if evt['event'] == zmq.EVENT_DISCONNECTED:
                        onDisconnect()
                        break
                gevent.sleep(1)
        spawn(invokeOnEvent, socketMonitor, self.onDisconnect)
        gevent.sleep(0)

    @timing
    def invokeAdapter(self):
        self.socket.send(self.content)
        result = self.socket.poll(self.pollTimeout)
        polls = 0
        while (result == 0 and polls < self.timeout and not self.disconnected):
            gevent.sleep(0.1)
            polls += self.pollTimeout
            result = self.socket.poll(self.pollTimeout)
        if (polls >= self.timeout):
            raise Exception('Timed out:' + str(self.timeout))
        if(self.disconnected):
            raise Exception('Disconnected')

        message = self.socket.recv()
        self.disconnected = True
        return message

    def onDisconnect(self):
        self.disconnected = True

    def close(self):
        self.socket.close()
