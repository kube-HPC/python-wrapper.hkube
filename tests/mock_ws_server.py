from websocket_server import WebsocketServer
from hkube_python_wrapper.util.encoding import Encoding
from tests.mocks import mockdata
from tests.configs import config
from hkube_python_wrapper.storage.storage_manager import StorageManager


class WebSocketServerClass:
    def __init__(self, encoding, server):
        sm = StorageManager(config.storage)
        storageEncoding = Encoding(config.storage.get('encoding'))
        self._server = server
        self._server.set_fn_new_client(self.handleConnected)
        self._server.set_fn_client_left(self.handleDisconnected)
        self._server.set_fn_message_received(self.handleMessage)
        self._encoding = Encoding(encoding)
        def getAlgorithmResult(request):
            execId = request.get('execId')
            storage= request.get('storage')
            storageInput= request.get('storageInput')
            input= request.get('input')
            if (not storageInput):
                response=input
            else:
                oneInput = storageInput[0]
                key = oneInput[2:]
                storageInfo=storage.get(key).get('storageInfo')
                (header,encoded)=sm.storage.get(storageInfo)
                decoded=storageEncoding.decode(header=header, value=encoded)
                response=[decoded]
            return {
                'execId': execId,
                'storage': storage,
                'response': response
            }
        self._commands = {
            "initialized":  {
                'command': "start",
                'data': lambda x: x
            },
            "startAlgorithmExecution": {
                'command': "algorithmExecutionDone",
                'data': getAlgorithmResult
            },
            "startStoredSubPipeline": {
                'command': "subPipelineDone",
                'data': lambda x: {
                    'subPipelineId': x.get('subPipelineId'),
                    'response': x.get('subPipeline').get('flowInput')
                }
            }
        }

    def handleMessage(self, client, server, message):
        decoded = self._encoding.decode(value=message, plainEncode=True)
        command = decoded["command"]
        data = decoded.get("data", None)
        commandBack = self._commands.get(command)
        if(commandBack):
            msgBack = {
                "command": commandBack["command"],
                "data": commandBack["data"](data)
            }
            self.sendMsgToClient(client, msgBack)

    def handleConnected(self, client, server):
        # print('ws connected')
        self.sendMsgToClient(client, {'command': 'initialize', 'data': mockdata.initData})

    def handleDisconnected(self, client, server):
        # print('ws disconnected')
        pass

    def sendMsgToClient(self, client, data):
        self._server.send_message(client, self._encoding.encode(data, plainEncode=True))


def startWebSocketServer(options):
    port = options["port"]
    encoding = options["encoding"]
    server = WebsocketServer(int(port))
    wss = WebSocketServerClass(encoding, server)
    server.run_forever()


if __name__ == "__main__":
    startWebSocketServer(config.socket)
