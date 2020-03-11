from storage.storage_manager import StorageManager
import os


class TaskOutputManager(StorageManager):
    storagePrefix = 'hkube'


    def __init__(self, adpater, config):
        super(TaskOutputManager,self).__init__(adpater)
        self.clusterName = config['clusterName']

    def put(self, jobId, taskId, data):
        return super(TaskOutputManager,self).put({'path': self.createPath(jobId, taskId), 'data': data})

    def get(self, jobId, taskId):
        return super(TaskOutputManager,self).get({'path': self.createPath(jobId, taskId)})

    def list(self, jobId):
        return super(TaskOutputManager,self).list({'path': self.createPath(jobId)})

    def delete(self, jobId, taskId=''):
        return super(TaskOutputManager,self).delete({'path': self.createPath(jobId, taskId)})

    def createPath(self, jobId, taskId=''):
        return self.clusterName + '-' + self.storagePrefix + os.path.sep + jobId + os.path.sep + taskId
