import traceback
import datetime
from gevent import sleep
from hkube_python_wrapper.communication.zmq.ZMQServer import ZMQServer
from hkube_python_wrapper.util.encoding import Encoding
import hkube_python_wrapper.util.object_path as objectPath
from hkube_python_wrapper.util.decorators import timing
import hkube_python_wrapper.util.type_check as typeCheck

MAX_CACHE_BYTES = 1024 * 1024 * 20


class CustomCache:
    def __init__(self, config):
        self._cache = dict()
        self._maxCacheSize = config.get('maxCacheSize')

    def update(self, key, value):
        if key not in self._cache and len(self._cache) >= self._maxCacheSize:
            self._remove_oldest()
        self._cache[key] = {'timestamp': datetime.datetime.now(), 'value': value}

    def __contains__(self, key):
        return key in self._cache

    def _remove_oldest(self):
        oldest = None
        for key in self._cache:
            if oldest is None:
                oldest = key
            elif self._cache[key]['timestamp'] < self._cache[oldest]['timestamp']:
                oldest = key
        self._cache.pop(oldest)

    def get(self, key):
        item = self._cache[key]
        return item.get('value')


class DataServer:

    def __init__(self, config):
        self._adapter = ZMQServer()
        self._cache = CustomCache(config)
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
            decoded = self._encoding.decode(message, plain_encode=True)
            tasks = decoded.get('tasks')
            taskId = decoded.get('taskId')
            datapath = decoded.get('path')
            result = self.createData(taskId, tasks, datapath)

        except Exception as e:
            result = self._createError('unknown', str(e))

        return self._encoding.encode(result)

    def createData(self, taskId, tasks, datapath):
        if(taskId is not None):
            return self.getDataByTaskId(taskId, datapath)

        errors = False
        items = []

        for taskId in tasks:
            result = self.getDataByTaskId(taskId, datapath)
            if(typeCheck.isDict(result) and 'hkube_error' in result):
                errors = True

            items.append(result)

        return dict({"items": items, "errors": errors})

    def getDataByTaskId(self, taskId, datapath):
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

    def isServing(self):
        return self._adapter.isServing()

    def waitTillServingEnds(self):
        self._adapter.stop()
        while(self.isServing()):
            sleep(1)
        self._adapter.close()

    def close(self):
        self._adapter.close()
