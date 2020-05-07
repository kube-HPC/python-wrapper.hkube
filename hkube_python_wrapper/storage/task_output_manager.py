import os
from hkube_python_wrapper.storage.base_storage_manager import BaseStorageManager


class TaskOutputManager(BaseStorageManager):
    storagePrefix = 'hkube'

    def __init__(self, adapter, config):
        super(TaskOutputManager, self).__init__(adapter)
        self.clusterName = config['clusterName']

    def put(self, jobId, taskId, data):
        return super(TaskOutputManager, self).put({'path': self.createPath(jobId, taskId), 'data': data})

    def get(self, jobId, taskId):
        return super(TaskOutputManager, self).get({'path': self.createPath(jobId, taskId)})

    def list(self, jobId):
        return super(TaskOutputManager, self).list({'path': self.createPath(jobId)})

    def delete(self, jobId, taskId=''):
        return super(TaskOutputManager, self).delete({'path': self.createPath(jobId, taskId)})

    def createPath(self, jobId, taskId=''):
        return self.clusterName + '-' + self.storagePrefix + os.path.sep + jobId + os.path.sep + taskId
