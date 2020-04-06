import traceback
from communication.zmq.ZMQServer import ZMQServer
from util.encoding import Encoding
import util.object_path as objectPath


class DataServer:

    def __init__(self, config):
        self._adapter = ZMQServer()
        self._task = None
        self._data = None
        self._host = config['host']
        self._port = config['port']
        self._encodingType = config['encoding']
        self._encoding = Encoding(self._encodingType)

    def listen(self):
        print('discovery serving on {host}:{port} with {encoding} encoding'.format(
            host=self._host, port=self._port, encoding=self._encodingType))
        self._adapter.listen(self._port, self._createReply)

    def _createReply(self, message):
        try:
            decodedMessage = self._encoding.decode(message)
            result = self.createData(decodedMessage)
        except Exception as e:
            traceback.print_exc()
            result = self._createError('unknown', str(e))
        finally:
            return self._encoding.encode(result)

    def createData(self, message):
        taskId = message['taskId']
        if(taskId != self._task):
            result = self._createError('notAvailableXX', 'Current taskId is ' + str(self._task))
        else:
            datapath = message['dataPath']
            data = self._data
            if(datapath):
                data = objectPath.getPath(self._data, datapath)
                if(data == 'DEFAULT'):
                    result = self._createError('noSuchDataPath', '{datapath} does not exist in data'.format(datapath=datapath))
                else:
                    result = {'data': data}
            else:
                result = {'data': data}
        return result

    def setSendingState(self, task, data):
        self._task = task
        self._data = data

    def endSendingState(self):
        self._task = None
        self._data = None

    def _createError(self, code, message):
        return {'error': {'code': code, 'message': message}}

    def isServing(self):
        return self._adapter.isServing()

    def close(self):
        self._adapter.close()
