from __future__ import print_function, division, absolute_import
from storage.task_output_manager import TaskOutputManager
from storage.base_storage_manager import BaseStorageManager
from storage.fsAdapter import FSAdapter
from storage.s3Adapter import S3Adapter

adapterTypes = {
    'fs': FSAdapter,
    's3': S3Adapter
}


class StorageManager():
    def __init__(self, config):
        storageType = config["type"]
        adapterConfig = config[storageType]
        adapterType = adapterTypes.get(storageType)
        adapter = adapterType(adapterConfig)
        self.hkube = TaskOutputManager(adapter, config)
        self.storage = BaseStorageManager(adapter)
        print('init {type} storage client'.format(type=storageType))
