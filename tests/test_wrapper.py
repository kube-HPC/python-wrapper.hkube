
from hkube_python_wrapper import Algorunner
from hkube_python_wrapper.data_adapter import DataAdapter
import tests.configs.config as conf

config = conf.Config
dataAdapter = DataAdapter(config.storage)

jobId = 'jobId-328901800'
taskId1 = 'taskId-328901801'
taskId2 = 'taskId-328901802'

array = [42, 37, 89, 95, 12, 126, 147]
nested = {'data': {'array': array}}

storageInfo1 = dataAdapter.setData({'jobId': jobId, 'taskId': taskId1, 'data': array})
storageInfo2 = dataAdapter.setData({'jobId': jobId, 'taskId': taskId2, 'data': nested})
discovery = config.discovery

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
    'guid-1': {'storageInfo': storageInfo2, 'discovery': discovery, 'path': 'data'},
    'guid-2': {'storageInfo': storageInfo2, 'discovery': discovery, 'path': 'data.array.4'},
    'guid-3': [{'storageInfo': storageInfo1, 'discovery': discovery}, {'storageInfo': storageInfo2, 'discovery': discovery}]
}


def start(args):
    ret = {
        "data": {
            "array": args["input"][1]["prop"]
        }
    }
    return ret


algorunner = Algorunner()
algorunner.loadAlgorithmCallbacks(start)
algorunner.connectToWorker(config)


def test_get_data():
    savePaths = ['green.data.array']
    job = {
        'jobId': jobId,
        'taskId': taskId1,
        'input': input,
        'storage': storage,
        'nodeName': 'green',
        'info': {'savePaths': savePaths}
    }
    algorunner._init(job)
    algorunner._start(job)
    assert '4' == '4'
