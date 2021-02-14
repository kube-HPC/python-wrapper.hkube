from __future__ import print_function, division, absolute_import
from hkube_python_wrapper.storage.task_output_manager import TaskOutputManager
from hkube_python_wrapper.storage.base_storage_manager import BaseStorageManager
from hkube_python_wrapper.storage.fs_adapter import FSAdapter
from hkube_python_wrapper.storage.s3_adapter import S3Adapter
from hkube_python_wrapper.util.logger import log

adapterTypes = {
    'fs': FSAdapter,
    's3': S3Adapter
}


class StorageManager():
    def __init__(self, config):
        storageType = config["type"]
        encoding = config["encoding"]
        adapterConfig = config[storageType]
        adapterType = adapterTypes.get(storageType)
        adapter = adapterType(adapterConfig, encoding)
        self.hkube = TaskOutputManager(adapter, config)
        self.storage = BaseStorageManager(adapter)
        log.info('init {type} storage client with {encoding} encoding', type=storageType, encoding=encoding)
