from __future__ import print_function, division, absolute_import
from storage.task_output_manager import TaskOutputManager
from storage.base_storage_manager import BaseStorageManager
from storage.fsAdapter import FSAdapter

adapterTypes = {
    'fs': FSAdapter,
    's3': FSAdapter
}


class StorageManager():
    def __init__(self, config):
        storageType = config["type"]
        specificConfig = config[storageType]
        adapterType = adapterTypes.get(storageType)
        adaptersConfig = dict()
        adaptersConfig.update(specificConfig)
        adaptersConfig.update(config)
        adapter = adapterType(adaptersConfig)
        self.hkube = TaskOutputManager(adapter, config)
        self.storage = BaseStorageManager(adapter)