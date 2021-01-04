import time
import types

from hkube_python_wrapper.storage.storage_manager import StorageManager
from hkube_python_wrapper.util.encoding import Encoding
from hkube_python_wrapper import Algorunner
from tests.configs import config
from hkube_python_wrapper.tracing.tracer import Tracer

algorithmName = 'eval-alg'
subpipelineName = 'simple'

encoding = Encoding(config.storage['encoding'])
inp1 =  {'storageInfo': {'path': 'c'}}
inp2 = {'storageInfo': {'path': 'a'}}
storageMock = {'a': [{'nodeName': 'node1','info':{'path':'b','isBigData':True}}],'b':'stam','c': [{'nodeName': 'node1','result':'originalRelsult'}]}
outp2 =  [{'nodeName': 'node1','info':{'path':'b','isBigData':True},'result':'stam'}]

def start(args, hkubeApi=None):
    # print('start called')
    waiter1 = hkubeApi.start_algorithm(algorithmName, inp1)
    waiter2 = hkubeApi.start_stored_subpipeline(subpipelineName, inp2)
    res = [waiter1, waiter2]
    return res


def storageManagerMocke(algorunner, options):
    if (algorunner._storage != 'v1'):
        algorunner._initDataServer(options)
    algorunner._initDataAdapter(options)
    algorunner._dataAdapter.tryGetDataFromPeerOrStorage = lambda response: storageMock.get(response.get('storageInfo').get('path'))


def test_callback():
    sm = StorageManager(config.storage)
    algorunner = Algorunner()

    algorunner._initStorage = types.MethodType(storageManagerMocke, algorunner)
    algorunner.loadAlgorithmCallbacks(start, options=config)
    algorunner.connectToWorker(config)
    time.sleep(3)
    (header, payload) = sm.storage.get({"path": "local-hkube/jobId/taskId"})
    decoded = encoding.decode(header=header, value=payload)
    assert decoded[0] == storageMock.get('c')
    assert decoded[1] == outp2
    time.sleep(2)
    Tracer.instance.close()
    algorunner.close()
