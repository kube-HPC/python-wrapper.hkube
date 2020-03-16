from SimpleWebSocketServer import SimpleWebSocketServer, WebSocket
import gevent
from gevent import monkey
from util.encoding import Encoding
monkey.patch_all()

initData = {
    'jobId': 'jobId',
    'taskId': 'taskId1',
    'input': [1, False, None],
    'nodeName': 'green'
}


class WebSocketServer(WebSocket):
    def init(self):
        self._encoding = Encoding(encoding)
        self._switcher = {
            "initialized": lambda data: self.sendMsgToClient({'command': 'start'}),
            "startAlgorithmExecution": lambda data: self.sendMsgToClient({'command': 'algorithmExecutionDone', 'data': initData}),
            "startStoredSubPipeline": lambda data: self.sendMsgToClient({'command': 'subPipelineDone', 'data': initData})
        }

    def handleMessage(self):
        decoded = self._encoding.decode(self.data)
        command = decoded["command"]
        data = decoded.get("data", None)
        func = self._switcher.get(command)
        gevent.spawn(func, data)

    def handleConnected(self):
        print(self.address, 'connected')
        self.init()
        self.sendMsgToClient({'command': 'initialize', 'data': initData})

    def handleClose(self):
        print(self.address, 'closed')

    def sendMsgToClient(self, data):
        self.sendMessage(self._encoding.encode(data))


def startWebSocketServer(options):
    global encoding
    port = options["port"]
    encoding = options["encoding"]
    server = SimpleWebSocketServer('', port, WebSocketServer)
    gevent.spawn(server.serveforever)
    return server
