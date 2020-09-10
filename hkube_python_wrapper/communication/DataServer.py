from hkube_python_wrapper.communication.zmq.ZMQServers import ZMQServers
from hkube_python_wrapper.util.encoding import Encoding
from hkube_python_wrapper.util.decorators import timing
from hkube_python_wrapper.cache.caching import Cache


class DataServer:

    def __init__(self, config):
        self._cache = Cache(config)
        self._host = config['host']
        self._port = config['port']
        self._encodingType = config['encoding']
        self._encoding = Encoding(self._encodingType)
        self._adapter = ZMQServers(self._port, self._createReply)
        self.notAvailable = self._encoding.encode(
            self._createError('notAvailable', 'taskId notAvailable'))

    def listen(self):
        print('discovery serving on {host}:{port} with {encoding} encoding'.format(
            host=self._host, port=self._port, encoding=self._encodingType))
        self._adapter.listen()

    @timing
    def _createReply(self, message):
        try:
            decoded = self._encoding.decode(message, plain_encode=True)
            tasks = decoded.get('tasks')
            results = self.getDataByTaskId(tasks)

        except Exception as e:
            result = self._createError('unknown', str(e))
            encoded = self._encoding.encode(result)
            return [encoded]
        parts = []
        for content in results:
            parts.append(content)
        return parts

    def getDataByTaskId(self, tasks):
        results = []
        for task in tasks:
            if (task not in self._cache):
                result = self.notAvailable
            else:
                result = self._cache.get(task)
            results.append(result)
        return results

    def setSendingState(self, taskId, encoded, size):
        return self._cache.update(taskId, encoded, size=size)

    def _createError(self, code, message):
        return {'hkube_error': {'code': code, 'message': message}}

    def isLocal(self, host, port):
        return host == self._host and port == self._port

    def isServing(self):
        return self._adapter.isServing()

    def shutDown(self):
        self._adapter.close()
