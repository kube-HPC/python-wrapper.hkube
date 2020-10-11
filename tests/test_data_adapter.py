import hkube_python_wrapper.util.type_check as typeCheck
from hkube_python_wrapper.wrapper.data_adapter import DataAdapter
from hkube_python_wrapper.communication.DataServer import DataServer
from tests.configs import config

dataAdapter = DataAdapter(config)

jobId = 'jobId-328901800'
taskId1 = 'taskId-328901801'
taskId2 = 'taskId-328901802'
taskId3 = 'taskId-328901803'
taskId4 = 'taskId-328901804'

array = [42, 37, 89, 95, 12, 126, 147]
obj1 = {'data1': {'array1': array}}
obj2 = {'data2': {'array2': array}}
obj3 = {'data3': {'array3': array}}
obj4 = {'data4': {'array4': array}}

(header1, data1) = dataAdapter.encode(obj1)
(header2, data2) = dataAdapter.encode(obj2)
(header3, data3) = dataAdapter.encode(obj3)
(header4, data4) = dataAdapter.encode(obj4)

storageInfo1 = dataAdapter.setData({'jobId': jobId, 'taskId': taskId1, 'header': header1, 'data': data1})
storageInfo2 = dataAdapter.setData({'jobId': jobId, 'taskId': taskId2, 'header': header2, 'data': data2})
storageInfo3 = dataAdapter.setData({'jobId': jobId, 'taskId': taskId3, 'header': header3, 'data': data3})
storageInfo4 = dataAdapter.setData({'jobId': jobId, 'taskId': taskId4, 'header': header4, 'data': data4})

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
    'guid-1': {'storageInfo': storageInfo1, 'path': 'data1'},
    'guid-2': {'storageInfo': storageInfo2, 'path': 'data2.array2'},
    'guid-3': {'storageInfo': storageInfo3, 'path': 'data3.array3.4'},
    'guid-4': {'storageInfo': storageInfo4},
    'guid-5': {'storageInfo': storageInfo4, 'path': 'data4.array4'}
}

discovery = dict(config.discovery)
discovery.update({"port": 9025})
ds = DataServer(discovery)
ds.listen()
ds.setSendingState(taskId1, header1, data1, 10)
ds.setSendingState(taskId2, header2, data2, 10)
ds.setSendingState(taskId3, header3, data3, 10)
ds.setSendingState(taskId4, header4, data4, 10)


def test_get_data_no_storage():
    result = dataAdapter.getData({'input': inputArgs})
    assert result == inputArgs


def test_get_data_no_input():
    result = dataAdapter.getData({'storage': storage})
    assert result is None


def test_get_data():
    result = dataAdapter.getData({'input': inputArgs, 'flatInput': flatInput, 'storage': storage})
    assert result[0]['data']['array1'] == array
    assert result[1]['prop'][0] == array
    assert result[2][0]['prop'] == array[4]
    assert result[3][0] == obj4
    assert result[4] == array
    assert result[5] == inputArgs[5]
    assert result[6] == inputArgs[6]
    assert result[7] == inputArgs[7]
    assert result[8] == inputArgs[8]


def test_get_batch_request_success():
    inputArgs = ['$$guid-1',]
    flatInput = {'0': '$$guid-5'}
    storage = {'guid-5': [{'discovery': discovery, 'tasks': [taskId2, taskId3, taskId4]},
                          {'discovery': discovery, 'tasks': [taskId2, taskId3, taskId4]},
                          {'discovery': discovery, 'tasks': [taskId2, taskId3, taskId4]},
                          {'discovery': discovery, 'tasks': [taskId2, taskId3, taskId4]}]}

    result = dataAdapter.getData({'input': inputArgs, 'flatInput': flatInput, 'storage': storage})
    assert result[0] == [obj2, obj3, obj4, obj2, obj3, obj4, obj2, obj3, obj4, obj2, obj3, obj4]


def test_get_peer_request_success():
    inputArgs = [
        '$$guid-1',
    ]
    flatInput = {
        '0': '$$guid-5'
    }
    storage = {'guid-5': [{'discovery': discovery, 'tasks': [taskId2, taskId3, taskId4]}]}
    result = dataAdapter.getData({'input': inputArgs, 'flatInput': flatInput, 'storage': storage})
    assert result[0] == [obj2, obj3, obj4]


def test_get_request():
    inputArgs = ['$$guid-1',]
    flatInput = {'0': '$$guid-5'}
    storage = {
        'guid-5': {'discovery': discovery, 'tasks': [taskId1]}
    }
    result = dataAdapter.getData({'jobId': jobId, 'input': inputArgs, 'flatInput': flatInput, 'storage': storage})
    assert result[0] == obj1


def test_get_local_request():
    inputArgs = ['$$guid-1',]
    flatInput = {'0': '$$guid-5'}
    storage = {
        'guid-5': {'discovery': discovery, 'tasks': [taskId1]}
    }
    dataAdapter._dataServer = ds
    result = dataAdapter.getData({'jobId': jobId, 'input': inputArgs, 'flatInput': flatInput, 'storage': storage})
    dataAdapter._dataServer = None
    assert result[0] == obj1


def test_get_batch_request_with_errors():
    inputArgs = ['$$guid-1',]
    flatInput = {'0': '$$guid-5'}
    storage = {
        'guid-5': [{'discovery': discovery, 'tasks': [taskId1, taskId2, taskId3, taskId4]}]
    }
    result = dataAdapter.getData({'jobId': jobId, 'input': inputArgs, 'flatInput': flatInput, 'storage': storage})
    assert result[0] == [obj2, obj3, obj4, obj1]


def test_get_batch_request_with_storage_fallback():
    ds.shutDown()
    inputArgs = [
        '$$guid-1',
    ]
    flatInput = {
        '0': '$$guid-5'
    }
    storage = {
        'guid-5': [{'discovery': discovery, 'tasks': [taskId1, taskId2, taskId3, taskId4]}]
    }

    result = dataAdapter.getData({'jobId': jobId, 'input': inputArgs, 'flatInput': flatInput, 'storage': storage})
    assert result[0] == [obj1, obj2, obj3, obj4]


def test_set_data():
    result = dataAdapter.setData({'jobId': jobId, 'taskId': taskId1, 'header': header1, 'data': data1})
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
    assert result["green.prop.5"] == {"type": "int"}


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
    assert result["green.prop"] == {"type": "bytearray"}


def test_createStorageInfo():
    savePaths = ['green.data1.array1']
    result = dataAdapter.createStorageInfo({
        'jobId': jobId,
        'taskId': taskId1,
        'nodeName': 'green',
        'data': obj1,
        'savePaths': savePaths
    })
    metadata = result['metadata'][savePaths[0]]

    assert metadata.get('size') == len(array)
    assert metadata.get('type') == 'array'
    assert result['storageInfo']['path'].find(jobId) != -1
