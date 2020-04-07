from communication.zmq.ZMQRequest import ZMQRequest
from util.encoding import Encoding
import util.type_check as typeCheck

class DataRequest:

    def __init__(self, reqDetails):
        encoding = reqDetails['encoding']
        self.encoding = Encoding(encoding)
        flattenEncodedReqDetails = reqDetails['address']
        flattenEncodedReqDetails['content'] = self.encoding.encode({'taskId': reqDetails['taskId'], 'dataPath': reqDetails['dataPath']})
        self.adapter = ZMQRequest(flattenEncodedReqDetails)

    def invoke(self):
        response = self.adapter.invokeAdapter()
        if typeCheck.isBytearray(response):
            return response
        return self.encoding.decode(response)
