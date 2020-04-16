
import time
from hkube_python_wrapper import Algorunner
from storage.storage_manager import StorageManager
from tests.configs import config
from util.encoding import Encoding

algorithmName = 'eval-alg'
subpipelineName = 'simple'

encoding = Encoding(config.storage['encoding'])


def start(args, hkubeApi=None):
    print('start called')
    waiter1 = hkubeApi.start_algorithm(algorithmName, [5, 10], resultAsRaw=True)
    waiter2 = hkubeApi.start_stored_subpipeline(subpipelineName, {'d': [6, 'stam']})
    res = [waiter1.get(), waiter2.get()]
    return res


def test_callback():
    sm = StorageManager(config.storage)
    config.discovery.update({"port": "9022"})
    algorunner = Algorunner()
    algorunner.loadAlgorithmCallbacks(start)
    algorunner.connectToWorker(config)
    time.sleep(2)
    data = sm.storage.get({"path": "local-hkube/jobId/taskId"})
    data = encoding.decode(data)
    assert data[0]['algorithmName'] == algorithmName
    assert data[1]['subPipeline']['name'] == subpipelineName
