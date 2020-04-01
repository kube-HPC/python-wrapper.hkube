
from websocket_server import WebsocketServer
from util.encoding import Encoding
from tests.mocks import mockdata
from tests.configs import config


class WebSocketServerClass:
    def __init__(self, encoding, server):
        self._server = server
        self._server.set_fn_new_client(self.handleConnected)
        self._server.set_fn_client_left(self.handleDisconnected)
        self._server.set_fn_message_received(self.handleMessage)
        self._encoding = Encoding(encoding)
        self._commands = {
            "initialized":  "start",
            "startAlgorithmExecution": "algorithmExecutionDone",
            "startStoredSubPipeline": "subPipelineDone"
        }

    def handleMessage(self, client, server, message):
        decoded = self._encoding.decode(message)
        command = decoded["command"]
        data = decoded.get("data", None)
        commandBack = self._commands.get(command)
        if(commandBack):
            msgBack = {
                "command": commandBack,
                "data": data
            }
            self.sendMsgToClient(client, msgBack)

    def handleConnected(self, client, server):
        print('connected')
        self.sendMsgToClient(client, {'command': 'initialize', 'data': mockdata.initData})

    def handleDisconnected(self, client, server):
        print('closed')

    def sendMsgToClient(self, client, data):
        self._server.send_message(client, self._encoding.encode(data))


def startWebSocketServer(options):
    port = options["port"]
    encoding = options["encoding"]
    server = WebsocketServer(int(port))
    wss = WebSocketServerClass(encoding, server)
    server.run_forever()


startWebSocketServer(config.socket)
