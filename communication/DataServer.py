from communication.zmq.ZMQServer import ZMQServer
from util.encoding import Encoding
import util.object_path as objectPath
import traceback


class DataServer:

    def __init__(self, config):
        self.adpater = ZMQServer(config, self.createReply)
        self.host = config['host']
        self.port = config['port']
        encoding = config['encoding']
        self.encoding = Encoding(encoding)

    def createReply(self, message):
        try:
            decodedMessage = self.encoding.decode(message)
            result = createData(decodedMessage)
        except Exception as e:
            traceback.print_exc()
            result = self.createError('unknown', str(e))
        finally:
            return self.encoding.encode(result)

    def createData(message):
        taskId = message['taskId']
        if(taskId != self.task):
            result = self.createError('notAvailable', 'Current taskId is ' + str(self.task))
        else:
            datapath = message['dataPath']
            data = self.data
            if(datapath):
                data = objectPath.getPath(self.data, datapath)

            result = {'data': data}
        return result

    def setSendingState(self, task, data):
        self.task = task
        self.data = data

    def endSendingState(self):
        self.task = None
        self.data = None

    def createError(self, code, message):
        return {'error': {'code': code, 'message': message}}

    def isServing(self):
        return self.adpater.isServing()

    def close(self):
        self.adpater.close()
