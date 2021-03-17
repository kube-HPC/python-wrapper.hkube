import time
from hkube_python_wrapper.storage.storage_manager import StorageManager
from hkube_python_wrapper.util.encoding import Encoding
from hkube_python_wrapper import Algorunner
from tests.configs import config
from hkube_python_wrapper.tracing.tracer import Tracer

algorithmName = 'eval-alg'
subpipelineName = 'simple'

encoding = Encoding(config.storage['encoding'])
inp1 =  {'nodeName': 'node1','result':'originalRelsult'}
inp2 = {'storageInfo': {'path': 'a'}}
storageMock = {
              'a': [{'nodeName': 'node1','info':{'path':'b','isBigData':True}}],
              'b':'stam',
              }
outp2 =  [{'nodeName': 'node1','info':{'path':'b','isBigData':True},'result':'stam'}]

def start(args, hkubeApi=None):
    waiter1 = hkubeApi.start_algorithm(algorithmName, [inp1])
    waiter2 = hkubeApi.start_stored_subpipeline(subpipelineName, inp2)
    res = [waiter1, waiter2]
    return res


def test_callback():
    sm = StorageManager(config.storage)
    algorunner = Algorunner()

    algorunner.loadAlgorithmCallbacks(start, options=config)
    algorunner.connectToWorker(config)
    origGet = algorunner._dataAdapter.tryGetDataFromPeerOrStorage
    def mockStorageGet(response):
        return storageMock.get(response.get('storageInfo').get('path'))
    algorunner._dataAdapter.tryGetDataFromPeerOrStorage = mockStorageGet
    time.sleep(3)
    (header, payload) = sm.storage.get({"path": "local-hkube/jobId/taskId"})
    algorunner._dataAdapter.tryGetDataFromPeerOrStorage = origGet
    decoded = encoding.decode(header=header, value=payload)
    assert decoded[0][0] == inp1
    assert decoded[1] == outp2
    time.sleep(2)
    Tracer.instance.close()
    algorunner.close()
