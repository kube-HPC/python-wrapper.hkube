from communication.zmq.ZMQRequest import ZMQRequest
from util.encoding import Encoding


class DataRequest:

    def __init__(self, reqDetails):
        encoding = reqDetails['encoding']
        self.encoding = Encoding(encoding)
        request = reqDetails['address']
        options = {
            u'taskId': reqDetails['taskId'],
            u'dataPath': reqDetails['dataPath']
        }
        request['content'] = self.encoding.encode(options, plain_encode=True)
        timeout = reqDetails['timeout']
        self.adapter = ZMQRequest(request)

    def invoke(self):
        try:
            response = self.adapter.invokeAdapter()
        except Exception as e:
            response = self._createError('unknown', e.message)
        self.adapter.close()
        return self.encoding.decode(response)

    def _createError(self, code, message):
        return {'error': {'code': code, 'message': message}}
