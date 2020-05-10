from hkube_python_wrapper.communication.zmq.ZMQRequest import ZMQRequest
from hkube_python_wrapper.util.encoding import Encoding


class DataRequest:

    def __init__(self, reqDetails):
        encoding = reqDetails['encoding']
        self.encoding = Encoding(encoding)
        request = reqDetails['address']
        options = {
            u'tasks': reqDetails['tasks'],
        }
        request['content'] = self.encoding.encode(options, plain_encode=True)
        request['timeout'] = reqDetails['timeout']
        self.adapter = ZMQRequest(request)

    def invoke(self):
        try:
            response = self.adapter.invokeAdapter()
        except Exception as e:
            return self._createError('unknown', e.message)
        self.adapter.close()
        return self.encoding.decode(response)

    def _createError(self, code, message):
        return {'hkube_error': {'code': code, 'message': message}}
