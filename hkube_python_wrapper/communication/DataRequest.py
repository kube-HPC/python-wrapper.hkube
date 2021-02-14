from hkube_python_wrapper.communication.zmq.ZMQRequest import ZMQRequest
from hkube_python_wrapper.util.encoding import Encoding
from hkube_python_wrapper.util.decorators import timing
from hkube_python_wrapper.util.logger import log


class DataRequest:
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
        content = self.encoding.encode(options, plainEncode=True)
        self.request = dict()
        self.request.update(address)
        self.request.update({"content": content, "timeout": timeout, "networkTimeout": networkTimeout})

    @timing
    def invoke(self):
        try:
            log.info('tcp://{host}:{port}', host=self.request['host'], port=self.request['port'])
            adapter = ZMQRequest(self.request)
            responseFrames = adapter.invokeAdapter()
            results = []
            for i in range(0, int(len(responseFrames)/2)):
                header = responseFrames[i*2]
                content = responseFrames[i*2+1]
                decoded = self.encoding.decode(header=header, value=content)
                results.append((len(content), decoded))
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
