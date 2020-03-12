import os
import sys
import pytest
from hkube_python_wrapper import Algorunner
from storage.storage_manager import StorageManager


config = {
    "storageMode": os.environ.get('STORAGE_MODE', 'byRef'),
    "socket": {
        "port": os.environ.get('WORKER_SOCKET_PORT', "3000"),
        "host": os.environ.get('WORKER_SOCKET_HOST', "127.0.0.1"),
        "protocol": os.environ.get('WORKER_SOCKET_PROTOCOL', "ws"),
        "url": os.environ.get('WORKER_SOCKET_URL', None),
        "encoding": os.environ.get('WORKER_ENCODING', 'bson')
    },
    "algorithmDiscovery": {
        "host": os.environ.get('POD_NAME', '127.0.0.1'),
        "port": os.environ.get('DISCOVERY_PORT', 9021),
        "encoding": os.environ.get('DISCOVERY_ENCODING', 'bson'),
    },
    "storage": {
        "encoding": os.environ.get('STORAGE_ENCODING', 'bson'),
        "clusterName": os.environ.get('CLUSTER_NAME', 'local'),
        "storageType": os.environ.get('STORAGE_TYPE', 'fs'),
        "fs": {
            "baseDirectory": os.environ.get('BASE_FS_ADAPTER_DIRECTORY', '/var/tmp/fs/storage')
        }
    }
}

storageManager = StorageManager(config["storage"])

jobId = 'jobId-328901800'
taskId1 = 'taskId-328901801'
taskId2 = 'taskId-328901802'

obj1 = [42, 37, 89, 95, 12, 126, 147]
obj2 = {'data': {'array': obj1}}

storageInfo1 = storageManager.hkube.put(jobId, taskId1, obj1)
storageInfo2 = storageManager.hkube.put(jobId, taskId2, obj2)

input = [
    {'data': '$$guid-1'},
    {'prop': ['$$guid-2']},
    [{'prop': '$$guid-3'}],
    'test-param',
    True,
    None,
    12345
]

storage = {
    'guid-1': {'storageInfo': storageInfo2, 'path': 'data'},
    'guid-2': {'storageInfo': storageInfo2, 'path': 'data.array', 'index': 4},
    'guid-3': [{'storageInfo': storageInfo1}, {'storageInfo': storageInfo2}]
}

def start(args):
    ret = {"foo": "bar"}
    return (ret)



algorunner = Algorunner()
algorunner.loadAlgorithmCallbacks(start)
algorunner.connectToWorker(config)
algorunner.initDataServer(config)
algorunner.initStorage(config)

def test_get_data_no_storage():
    savePaths = ['green.data.array']
    job = {
        'jobId':jobId,
        'taskId': taskId1,
        'input': input,
        'storage': storage,
        'nodeName': 'green',
        'info': {'savePaths': savePaths}
    }
    algorunner._init(job)
    algorunner._start(job)
    assert '4' == '4'

