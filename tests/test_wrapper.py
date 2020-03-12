import os
import sys
import pytest
from hkube_python_wrapper.data_adapter import DataAdapter
from storage.storage_manager import StorageManager


config = {
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
dataAdapter = DataAdapter()
dataAdapter.init(config)

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
    ['$$guid-4'],
    '$$guid-5',
    '$$guid-6',
    '$$guid-7',
    'test-param',
    True,
    None,
    12345
]

storage = {
    'guid-1': {'storageInfo': storageInfo2, 'path': 'data'},
    'guid-2': {'storageInfo': storageInfo2, 'path': 'data.array'},
    'guid-3': {'storageInfo': storageInfo2, 'path': 'data.array.4'},
    'guid-4': {'storageInfo': storageInfo2, 'path': 'data.array', 'index': 4},
    'guid-5': {'storageInfo': storageInfo1, 'index': 2},
    'guid-6': {'storageInfo': storageInfo1},
    'guid-7': [{'storageInfo': storageInfo1}, {'storageInfo': storageInfo2}]
}


def test_get_data_no_storage():
    result = dataAdapter.getData({'input': input})
    assert result == input


def test_get_data_no_input():
    result = dataAdapter.getData({'storage': storage})
    assert result == None


def test_get_data():

    result = dataAdapter.getData({'input': input, 'storage': storage})
    assert result[0]['data']['array'] == obj1
    assert result[1]['prop'][0] == obj1
    assert result[2][0]['prop'] == obj1[4]
    assert result[3][0] == obj1[4]
    assert result[4] == obj1[2]
    assert result[5] == obj1
    assert result[6] == [obj1, obj2]
    assert result[7] == input[7]
    assert result[8] == input[8]
    assert result[9] == input[9]
    assert result[10] == input[10]


def test_set_data():

    result = dataAdapter.setData({'jobId': jobId, 'taskId': taskId1, 'data': obj1})
    assert result['path']


def test_createStorageInfo():
    savePaths = ['green.data.array']
    result = dataAdapter.createStorageInfo({
        'jobId': jobId,
        'taskId': taskId1,
        'nodeName': 'green',
        'data': obj2,
        'savePaths': savePaths
    })
    metadata = result['metadata'][savePaths[0]]

    assert metadata.get('size') == len(obj1)
    assert metadata.get('type') == 'array'
    assert result['storageInfo']['path'].find(jobId) != -1
