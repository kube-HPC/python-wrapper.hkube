import traceback
from gevent import sleep
from hkube_python_wrapper.communication.zmq.ZMQServer import ZMQServer
from hkube_python_wrapper.util.encoding import Encoding
import hkube_python_wrapper.util.object_path as objectPath
from hkube_python_wrapper.util.decorators import timing

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

    @timing
    def _createReply(self, message):
        try:
            decodedMessage = self._encoding.decode(message, plain_encode=True)
            result = self.createData(decodedMessage)
        except Exception as e:
            traceback.print_exc()
            result = self._createError('unknown', str(e))
        finally:
            return result

    def createData(self, message):
        taskId = message['taskId']
        result = None
        if(taskId != self._task):
            result = self._createError('notAvailable', 'Current taskId is {task}'.format(task=str(self._task)))
        else:
            datapath = message['dataPath']
            result = self._data
            if(datapath):
                data = self._encoding.decode(self._data)
                result = objectPath.getPath(data, datapath)
                if(result == 'DEFAULT'):
                    result = self._createError('noSuchDataPath', '{datapath} does not exist in data'.format(datapath=datapath))
                else:
                    result = self._encoding.encode(result)
        return result

    def setSendingState(self, task, data):
        self._task = task
        self._data = data

    def endSendingState(self):
        self._task = None
        self._data = None

    def _createError(self, code, message):
        return self._encoding.encode({'hkube_error': {'code': code, 'message': message}})

    def isServing(self):
        return self._adapter.isServing()

    def waitTillServingEnds(self):
        self._adapter.stop()
        while(self.isServing()):
            sleep(1)
        self._adapter.close()
                     
    def close(self):
        self._adapter.close()
