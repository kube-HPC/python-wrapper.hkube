import gevent
import zmq
from gevent import spawn
from zmq.utils.monitor import recv_monitor_message

from util.decorators import timing

context = zmq.Context()


class ZMQRequest(object):
    def __init__(self, reqDetails):
        self.socket = context.socket(zmq.REQ)
        self.socket.connect('tcp://' + reqDetails['host'] + ':' + str(reqDetails['port']))
        self.connected = False
        self.content = reqDetails['content']
        self.timeout= int(reqDetails['timeout'])
        self.disconnected = False
        socketMoniotr = self.socket.get_monitor_socket()

        def invokeOnEvent(monitor, onDisconnect):
            while True:
                if(monitor.poll(0.1)):
                    evt = recv_monitor_message(monitor)
                    if evt['event'] == zmq.EVENT_DISCONNECTED:
                        onDisconnect()
                gevent.sleep(1)
        spawn(invokeOnEvent, socketMoniotr, self.onDisconnect)
        gevent.sleep(0)

    @timing
    def invokeAdapter(self):
        self.socket.send(self.content)
        result = self.socket.poll(1)
        polls = 0
        while (result == 0 and polls < self.timeout and not self.disconnected):
            gevent.sleep(1)
            polls += 1
            result = self.socket.poll(1)
        if (polls>=self.timeout):
            e = Exception()
            e.__setattr__('message','Timed out:' + str(self.timeout))
            raise e
        if(self.disconnected):
            raise Exception('Disconnected')
        message = self.socket.recv()
        return message

    def onDisconnect(self):
        self.disconnected = True

    def close(self):
        self.socket.close()
