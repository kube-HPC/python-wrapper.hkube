from communication.zmq.ZMQServer import ZMQServer
from util.encoding import Encoding
import dpath.util


class DataServer:

    def __init__(self,config):
        self.adpater = ZMQServer(config,self.createReply)
        encoding = config['encoding']
        self.encoding = Encoding(encoding)
    def createReply(self,message):
        try:
            decodedMessage = self.encoding.decode(message)
            taskId = decodedMessage['taskId']
            if(taskId != self.task):
                result =  self.createError('notAvailable','Current taskId is '+ str(self.task))
            else:
                datapath = decodedMessage['dataPath']
                if(datapath != None and len(str(datapath))>0):
                    result ={'data': dpath.util.get( self.data,datapath)}
                else:
                    result = {'data':self.data}
        except Exception as e:
            result = self.createError('unknown',str(e))
        finally:
            return self.encoding.encode(result)

    def setSendingState(self,task,data):
        self.task = task
        self.data = data

    def endSendingState(self):
        self.task = None
        self.data = None
    def createError(self,code,message):
        return {'error': {'code':code,'message':message}}