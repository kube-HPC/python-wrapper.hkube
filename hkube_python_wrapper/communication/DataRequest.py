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
        content = self.encoding.encode(options, plain_encode=True)
        self.request = dict()
        self.request.update(address)
        self.request.update({"content": content, "timeout": timeout, "networkTimeout": networkTimeout})

    def invoke(self):
        try:
            print('tcp://' + self.request['host'] + ':' + str(self.request['port']))
            adapter = ZMQRequest(self.request)
            responseFrames = adapter.invokeAdapter()
            header = None
            results = []
            for i in range(0,len(responseFrames)):
                header = responseFrames[1]
                content = responseFrames[0]
                decoded = self.encoding.decode2(header,content)
                results.append (len(content),decoded)
            return results
        except Exception as e:
            print ("exception " + str(e))
            return 0, self._createError('unknown', str(e))
        finally:
            adapter.close()

    def _createError(self, code, message):
        return {'hkube_error': {'code': code, 'message': message}}
