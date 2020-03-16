
import time
from hkube_python_wrapper import Algorunner
import tests.configs.config as conf
from gevent import monkey
from .mock_ws_server import startWebSocketServer, initData
monkey.patch_all()


config = conf.Config

startWebSocketServer(config.socket)


def start(args, hkubeApi=None):
    print('start called')
    # waiter1 = hkubeApi.start_algorithm('eval-alg', [5, bytearray(b'\xdd'*10)], resultAsRaw=True)
    waiter1 = hkubeApi.start_algorithm('eval-alg', [5, 10], resultAsRaw=True)

    waiter2 = hkubeApi.start_stored_subpipeline('simple', {'d': [6, 'stam']})
    res = [waiter1.get(), waiter2.get()]
    print('got all results')
    # ret = list(map(lambda x: {'error': x.get('error')} if x.get(
    #     'error') != None else {'response': x.get('response')}, res))
    # # ret='OK!!!'
    ret = {"foo": "bar"}
    return (ret)


def test_callback():
    alg = Algorunner()
    alg.loadAlgorithmCallbacks(start)
    job = alg.connectToWorker(config)
    time.sleep(1)
    alg._init(initData)
    alg._start({})
    job.join()
