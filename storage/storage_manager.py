from __future__ import print_function, division, absolute_import
from storage.task_output_manager import TaskOutputManager
from storage.base_storage_manager import BaseStorageManager
from storage.fsAdapter import FSAdapter
from storage.s3Adapter import S3Adapter
from util.encoding import Encoding

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
        encodingType = config['encoding']
        encoding = Encoding(encodingType)
        self.hkube = TaskOutputManager(adapter, config, encoding)
        self.storage = BaseStorageManager(adapter, encoding)
        print('init {type} storage client with {encoding} encoding'.format(type=storageType, encoding=encodingType))
