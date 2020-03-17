from communication.zmq.ZMQRequest import ZMQRequest
from util.encoding import Encoding


class DataRequest:

    def __init__(self, reqDetails):
        encoding = reqDetails['encoding']
        self.encoding = Encoding(encoding)
        flattenEncodedReqDetails = reqDetails['address']
        flattenEncodedReqDetails['content'] = self.encoding.encode({'taskId':reqDetails['taskId'],'dataPath':reqDetails['dataPath']})
        self.adapter = ZMQRequest(flattenEncodedReqDetails)

    def invoke(self):
        return self.encoding.decode(self.adapter.invoke())
