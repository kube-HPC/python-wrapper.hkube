from configs import config
import os
import simplejson as json
import sys
from hkube_python_wrapper import Algorunner, HKubeApi
import tests.configs.config as conf
from gevent import monkey
monkey.patch_all()


config = conf.Config


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


def main_callbacks():
    alg = Algorunner()
    alg.loadAlgorithmCallbacks(start)
    job = alg.connectToWorker(config)
    job.join()


def main_file():
    alg = Algorunner()
    alg.loadAlgorithm({
        "path": "test_alg",
        "entryPoint": "test_alg1.py"
    })
    job = alg.connectToWorker(config)
    job.join()


if __name__ == '__main__':
    main_callbacks()
