from hkube_python_wrapper.communication.zmq.ZMQRequest import ZMQRequest
from hkube_python_wrapper.util.encoding import Encoding


class DataRequest:

    def __init__(self, reqDetails):
        encoding = reqDetails.get('encoding')
        address = reqDetails.get('address')
        timeout = reqDetails.get('timeout')
        networkTimeout = reqDetails.get('networkTimeout')
        options = {
            u'tasks': reqDetails.get('tasks'),
            u'taskId': reqDetails.get('taskId'),
            u'dataPath': reqDetails.get('dataPath')
        }
        self.encoding = Encoding(encoding)
        header ,content = self.encoding.encode(options, plain_encode=True)
        self.request = dict()
        self.request.update(address)
        self.request.update({"content": content, "timeout": timeout, "networkTimeout": networkTimeout})

    def invoke(self):
        try:
            print('tcp://' + self.request['host'] + ':' + str(self.request['port']))
            adapter = ZMQRequest(self.request)
            responseFrames = adapter.invokeAdapter()
            header = None
            if (len(responseFrames) == 2):
                header = responseFrames[1]
            content = responseFrames[0]
            return (len(content), self.encoding.decode(content,header))
        except Exception as e:
            return 0, self._createError('unknown', str(e))
        finally:
            adapter.close()

    def _createError(self, code, message):
        return {'hkube_error': {'code': code, 'message': message}}
