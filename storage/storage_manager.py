from storage.task_output_manager import TaskOutputManager
from storage.base_storage_manager import BaseStorageManager
from storage.fsAdapter import FSAdapter

adapterTypes = {
    'fs': FSAdapter,
    's3': FSAdapter
}


class StorageManager():
    def __init__(self, config):
        storageType = config["storageType"]
        adapterConfig = config[storageType]
        adapterType = adapterTypes.get(storageType)
        adapter = adapterType({**adapterConfig, **config})
        self.hkube = TaskOutputManager(adapter, config)
        self.storage = BaseStorageManager(config)
