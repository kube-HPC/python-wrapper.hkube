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
        self.adapter = ZMQRequest(request)

    def invoke(self):
        response = self.adapter.invokeAdapter()
        return self.encoding.decode(response.bytes)
