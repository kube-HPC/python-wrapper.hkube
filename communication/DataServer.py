from communication.zmq.ZMQServer import ZMQServer
from util.encoding import Encoding
import dpath.util


class DataServer:

    def __init__(self,config):
        self.adpater = ZMQServer(config,self.createReply)
        encoding = config['encoding']
        self.encoding = Encoding(encoding)
    def createReply(self,message):
        decodedMessage = self.encoding.decode(message)
        result = dpath.util.get(self.data,decodedMessage['dataPath'])
        return  self.encoding.encode(result)
    def setSendingState(self,task,data):
        self.task = task
        self.data = data