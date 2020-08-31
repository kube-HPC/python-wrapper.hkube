
import time
from hkube_python_wrapper.storage.storage_manager import StorageManager
from hkube_python_wrapper.util.encoding import Encoding
from hkube_python_wrapper import Algorunner
from tests.configs import config
from hkube_python_wrapper.tracing.tracer import Tracer

algorithmName = 'eval-alg'
subpipelineName = 'simple'

encoding = Encoding(config.storage['encoding'])
inp1 = [5, 10]
inp2 = {'d': [6, 'stam']}


def start(args, hkubeApi=None):
    # print('start called')
    waiter1 = hkubeApi.start_algorithm(algorithmName, inp1)
    waiter2 = hkubeApi.start_stored_subpipeline(subpipelineName, inp2)
    res = [waiter1.get(), waiter2.get()]
    return res


def test_callback():
    sm = StorageManager(config.storage)
    algorunner = Algorunner()
    algorunner.loadAlgorithmCallbacks(start, options=config)
    algorunner.connectToWorker(config)
    time.sleep(3)
    res = sm.storage.get({"path": "local-hkube/jobId/taskId"})
    decoded = encoding.decode(res)
    assert decoded[0] == inp1
    assert decoded[1] == inp2
    time.sleep(2)
    Tracer.instance.close()
