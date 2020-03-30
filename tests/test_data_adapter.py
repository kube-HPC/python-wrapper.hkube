
from hkube_python_wrapper.data_adapter import DataAdapter
import tests.configs.config as conf
import util.type_check as typeCheck
import collections
config = conf.Config

dataAdapter = DataAdapter(config.storage)

jobId = 'jobId-328901800'
taskId1 = 'taskId-328901801'
taskId2 = 'taskId-328901802'

obj1 = [42, 37, 89, 95, 12, 126, 147]
obj2 = {'data': {'array': obj1}}

storageInfo1 = dataAdapter.setData({'jobId': jobId, 'taskId': taskId1, 'data': obj1})
storageInfo2 = dataAdapter.setData({'jobId': jobId, 'taskId': taskId2, 'data': obj2})

inputArgs = [
    {'data': '$$guid-1'},
    {'prop': ['$$guid-2']},
    [{'prop': '$$guid-3'}],
    ['$$guid-4'],
    '$$guid-5',
    'test-param',
    True,
    None,
    12345
]

flatInput = {
    '0.data': '$$guid-1',
    '1.prop.0': '$$guid-2',
    '2.0.prop': '$$guid-3',
    '3.0': '$$guid-4',
    '4': '$$guid-5',
    '5': 'test-param',
    '6': True,
    '7': None,
    '8': 12345
}

storage = {
    'guid-1': {'storageInfo': storageInfo2, 'path': 'data'},
    'guid-2': {'storageInfo': storageInfo2, 'path': 'data.array'},
    'guid-3': {'storageInfo': storageInfo2, 'path': 'data.array.4'},
    'guid-4': {'storageInfo': storageInfo1},
    'guid-5': [{'storageInfo': storageInfo1}, {'storageInfo': storageInfo2}]
}


def test_get_data_no_storage():
    result = dataAdapter.getData({'input': inputArgs})
    assert result == inputArgs


def test_get_data_no_input():
    result = dataAdapter.getData({'storage': storage})
    assert result == None


def test_get_data():

    result = dataAdapter.getData({'input': inputArgs, 'flatInput': flatInput, 'storage': storage})
    assert result[0]['data']['array'] == obj1
    assert result[1]['prop'][0] == obj1
    assert result[2][0]['prop'] == obj1[4]
    assert result[3][0] == obj1
    assert result[4] == [obj1, obj2]
    assert result[5] == inputArgs[5]
    assert result[6] == inputArgs[6]
    assert result[7] == inputArgs[7]
    assert result[8] == inputArgs[8]


def test_set_data():

    result = dataAdapter.setData({'jobId': jobId, 'taskId': taskId1, 'data': obj1})
    assert result['path'].find(jobId) != -1


def test_createMetadata_no_path():
    array = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    nodeName = 'green'
    savePaths = [nodeName + '.no.such']
    result = dataAdapter.createMetadata({
        'nodeName': nodeName,
        'data': {"prop": array},
        'savePaths': savePaths
    })
    assert typeCheck.isDict(result)


def test_createMetadata_array():
    array = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    nodeName = 'green'
    savePaths = [nodeName + '.prop.5']
    result = dataAdapter.createMetadata({
        'nodeName': nodeName,
        'data': {"prop": array},
        'savePaths': savePaths
    })

    return result


def test_createMetadata_bytes():
    sizeBytes = 100 * 1000000
    bytesData = bytearray(sizeBytes)
    nodeName = 'green'
    savePaths = [nodeName + '.prop']
    result = dataAdapter.createMetadata({
        'nodeName': nodeName,
        'data': {"prop": bytesData},
        'savePaths': savePaths
    })

    return result


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
