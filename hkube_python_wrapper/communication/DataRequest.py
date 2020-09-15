from hkube_python_wrapper.communication.zmq.ZMQRequest import ZMQRequest
from hkube_python_wrapper.util.encoding import Encoding


class DataRequest:
    ping_times = []

    def __init__(self, reqDetails):
        encoding = reqDetails.get('encoding')
        address = reqDetails.get('address')
        timeout = reqDetails.get('timeout')
        networkTimeout = reqDetails.get('networkTimeout')
        tasks = reqDetails.get('tasks')
        options = {
            u'tasks': tasks
        }
        self.tasks = tasks
        self.encoding = Encoding(encoding)
        content = self.encoding.encode(options, plain_encode=True)
        self.request = dict()
        self.request.update(address)
        self.request.update({"content": content, "timeout": timeout, "networkTimeout": networkTimeout})

    def invoke(self):
        try:
            print('tcp://' + self.request['host'] + ':' + str(self.request['port']))
            adapter = ZMQRequest(self.request)
            responseFrames = adapter.invokeAdapter()
            results = []
            for content in responseFrames:
                decoded = self.encoding.decode(content)
                results.append((len(content), decoded))
            DataRequest.ping_times.append(adapter.pingTime)
            return results
        except Exception as e:
            results = []
            for _ in self.tasks:
                results.append((0, self._createError('unknown', str(e))))
            return results
        finally:
            adapter.close()

    def _createError(self, code, message):
        return {'hkube_error': {'code': code, 'message': message}}
