import os
import simplejson as json
import sys
from hkube_python_wrapper import Algorunner, HKubeApi
from gevent import monkey
monkey.patch_all()


config = {
    "storageMode": os.environ.get('STORAGE_MODE', 'byRef'),
    "socket": {
        "port": os.environ.get('WORKER_SOCKET_PORT', "3000"),
        "host": os.environ.get('WORKER_SOCKET_HOST', "127.0.0.1"),
        "protocol": os.environ.get('WORKER_SOCKET_PROTOCOL', "ws"),
        "url": os.environ.get('WORKER_SOCKET_URL', None),
        "encoding": os.environ.get('WORKER_ENCODING', 'bson')
    },
    "algorithmDiscovery": {
        "host": os.environ.get('POD_NAME', '127.0.0.1'),
        "port": os.environ.get('DISCOVERY_PORT', 9020),
        "encoding": os.environ.get('DISCOVERY_ENCODING', 'bson'),
    },
    "storage": {
        "encoding": os.environ.get('STORAGE_ENCODING', 'bson'),
        "clusterName": os.environ.get('CLUSTER_NAME', 'local'),
        "storageType": os.environ.get('STORAGE_TYPE', 'fs'),
        "fs": {
            "baseDirectory": os.environ.get('BASE_FS_ADAPTER_DIRECTORY', '/var/tmp/fs/storage')
        }
    }
}


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
    alg.initStorage(config)
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