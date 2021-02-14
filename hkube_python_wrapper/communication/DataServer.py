from hkube_python_wrapper.communication.zmq.ZMQServers import ZMQServers
from hkube_python_wrapper.util.encoding import Encoding
from hkube_python_wrapper.util.decorators import timing
from hkube_python_wrapper.cache.caching import Cache
from hkube_python_wrapper.util.logger import log


class DataServer:

    def __init__(self, config):
        self._cache = Cache(config)
        self._host = config['host']
        self._port = config['port']
        self._encodingType = config['encoding']
        self._encoding = Encoding(self._encodingType)

        self._adapter = ZMQServers(self._port, self._createReply, config)
        self.notAvailable = self._encoding.encode(
            self._createError('notAvailable', 'taskId notAvailable'))

    def listen(self):
        log.info('discovery serving on {host}:{port} with {encoding} encoding', host=self._host, port=self._port, encoding=self._encodingType)
        self._adapter.listen()

    @timing
    def _createReply(self, message):
        try:
            decoded = self._encoding.decode(value=message, plainEncode=True)
            tasks = decoded.get('tasks')
            resultsAsTuple = self.getDataByTaskId(tasks)

        except Exception as e:
            result = self._createError('unknown', str(e))
            header, encoded = self._encoding.encode(result)
            return [header, encoded]
        parts = []
        for header, content in resultsAsTuple:
            parts.append(header)
            parts.append(content)
        return parts

    def getDataByTaskId(self, tasks):
        results = []
        for task in tasks:
            if (task not in self._cache):
                result = self.notAvailable
            else:
                result = self._cache.getWithHeader(task)
            results.append(result)
        return results

    def setSendingState(self, taskId, header, encoded, size):
        return self._cache.update(taskId, encoded, size=size, header=header)

    def _createError(self, code, message):
        return {'hkube_error': {'code': code, 'message': message}}

    def isLocal(self, host, port):
        return host == self._host and port == self._port

    def isServing(self):
        return self._adapter.isServing()

    def shutDown(self):
        self._adapter.close()
