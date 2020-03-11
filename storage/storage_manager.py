from storage.task_output_manager import TaskOutputManager
from storage.base_storage_manager import BaseStorageManager
from storage.fsAdapter import FSAdapter
class StorageManager():
    def __init__(self, config):
        adpater = FSAdapter()
        self.hkube = TaskOutputManager(adpater,config)
        self.storage = BaseStorageManager(adpater,config)