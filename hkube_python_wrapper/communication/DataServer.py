
from gevent import sleep
from hkube_python_wrapper.communication.zmq.ZMQServers import ZMQServers
from hkube_python_wrapper.util.encoding import Encoding
import hkube_python_wrapper.util.object_path as objectPath
from hkube_python_wrapper.util.decorators import timing
import hkube_python_wrapper.util.type_check as typeCheck
from hkube_python_wrapper.cache.caching import CustomCache

class DataServer:

    def __init__(self, config):
        self._cache = CustomCache(config)
        self._host = config['host']
        self._port = config['port']
        self._encodingType = config['encoding']
        self._encoding = Encoding(self._encodingType)
        self._adapter = ZMQServers(self._port, self._createReply)

    def listen(self):
        print('discovery serving on {host}:{port} with {encoding} encoding'.format(
            host=self._host, port=self._port, encoding=self._encodingType))
        self._adapter.listen()

    @timing
    def _createReply(self, message):
        try:
            decoded = self._encoding.decode(message, plain_encode=True)
            tasks = decoded.get('tasks')
            taskId = decoded.get('taskId')
            datapath = decoded.get('path')
            result = self.createData(taskId, tasks, datapath)

        except Exception as e:
            result = self._createError('unknown', str(e))

        return self._encoding.encode(result)

    @timing
    def createData(self, taskId, tasks, datapath):
        if(taskId is not None):
            return self._getDataByTaskId(taskId, datapath)

        errors = False
        items = []

        for task in tasks:
            result = self._getDataByTaskId(task, datapath)
            if(typeCheck.isDict(result) and 'hkube_error' in result):
                errors = True

            items.append(result)

        return dict({"items": items, "errors": errors})

    def _getDataByTaskId(self, taskId, datapath):
        result = None
        if(taskId not in self._cache):
            result = self._createError('notAvailable', 'taskId notAvailable')
        else:
            data = self._cache.get(taskId)
            result = self._createDataByPath(data, datapath)
        return result

    def _createDataByPath(self, data, datapath):
        if(datapath):
            data = objectPath.getPath(data, datapath)
            if(data == 'DEFAULT'):
                data = self._createError('noSuchDataPath', '{datapath} does not exist in data'.format(datapath=datapath))

        return data

    def setSendingState(self, taskId, data):
        self._cache.update(taskId, data)

    def _createError(self, code, message):
        return {'hkube_error': {'code': code, 'message': message}}

    def isLocal(self, host, port):
        return host == self._host and port == self._port

    def isServing(self):
        return self._adapter.isServing()

    def shutDown(self):
        while(self.isServing()):
            sleep(1)
        self._adapter.close()
        