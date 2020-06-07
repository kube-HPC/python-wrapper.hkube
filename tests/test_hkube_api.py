
import time
from hkube_python_wrapper.storage.storage_manager import StorageManager
from hkube_python_wrapper.util.encoding import Encoding
from hkube_python_wrapper import Algorunner
from tests.configs import config
from hkube_python_wrapper.tracing.tracer import Tracer

algorithmName = 'eval-alg'
subpipelineName = 'simple'

encoding = Encoding(config.storage['encoding'])


def start(args, hkubeApi=None):
    # print('start called')
    waiter1 = hkubeApi.start_algorithm(algorithmName, [5, 10])
    waiter2 = hkubeApi.start_stored_subpipeline(subpipelineName, {'d': [6, 'stam']})
    res = [waiter1.get(), waiter2.get()]
    return res


def test_callback():
    sm = StorageManager(config.storage)
    data = {"data": {"prop": "bla"}}
    encoded = encoding.encode(data, plain_encode=True)
    sm.storage.put({"path": "local-hkube/jobId_start_algorithm/taskId_start_algorithm", "data": encoded})
    config.discovery.update({"port": "9022"})
    algorunner = Algorunner()
    algorunner.loadAlgorithmCallbacks(start, options=config)
    algorunner.connectToWorker(config)
    time.sleep(2)
    res = sm.storage.get({"path": "local-hkube/jobId/taskId"})
    decoded = encoding.decode(res)
    assert decoded[0] == data
    assert decoded[1]['subPipeline']['name'] == subpipelineName
    time.sleep(2)
    Tracer.instance.close()
