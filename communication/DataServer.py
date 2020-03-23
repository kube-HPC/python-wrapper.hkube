from communication.zmq.ZMQServer import ZMQServer
from util.encoding import Encoding
import util.object_path as objectPath


class DataServer:

    def __init__(self, config):
        self.adpater = ZMQServer(config, self.createReply)
        encoding = config['encoding']
        self.encoding = Encoding(encoding)

    def createReply(self, message):
        try:
            decodedMessage = self.encoding.decode(message)
            taskId = decodedMessage['taskId']
            if(taskId != self.task):
                result = self.createError('notAvailable', 'Current taskId is ' + str(self.task))
            else:
                datapath = decodedMessage['dataPath']
                if(datapath):
                    decoded = self.encoding.decode(self.data)
                    data = decoded['data']
                    result = {'data': objectPath.getPath(data, datapath)}
                    result = self.encoding.encode(result)
                else:
                    result = self.data

        except Exception as e:
            result = self.createError('unknown', str(e))
        finally:
            return result

    def setSendingState(self, task, data):
        self.task = task
        self.data = data

    def endSendingState(self):
        self.task = None
        self.data = None

    def createError(self, code, message):
        error = {'error': {'code': code, 'message': message}}
        return self.encoding.encode(error)

    def isServing(self):
        return self.adpater.isServing()

    def close(self):
        self.adpater.close()
