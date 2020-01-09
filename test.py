import os
import simplejson as json
import sys
from hkube_python_wrapper import Algorunner, HKubeApi
from gevent import monkey
monkey.patch_all()


socket = {
    "port": os.environ.get('WORKER_SOCKET_PORT', "3000"),
    "host": os.environ.get('WORKER_SOCKET_HOST', "127.0.0.1"),
    "protocol": os.environ.get('WORKER_SOCKET_PROTOCOL', "ws"),
    "url": os.environ.get('WORKER_SOCKET_URL', None),
    "binary": os.environ.get('WORKER_BINARY', 'False'),
}


def start(args, hkubeApi=None):
    print('start called')
    waiter1 = hkubeApi.start_algorithm('eval-alg', [5, bytearray(b'\xdd'*10)], resultAsRaw=True)
    waiter2 = hkubeApi.start_stored_subpipeline('simple', {'d': [6, 'stam']})
    res = [waiter1.get(), waiter2.get()]
    print('got all results')
    ret = list(map(lambda x: {'error': x.get('error')} if x.get(
        'error') != None else {'response': x.get('response')}, res))
    # ret='OK!!!'
    return (ret,bytearray(b'\xcc'*10))



def main_callbacks():
    alg = Algorunner()
    alg.loadAlgorithmCallbacks(start)
    job = alg.connectToWorker(socket)
    job.join()

def main_file():
    alg = Algorunner()
    alg.loadAlgorithm({
        "path":"test_alg",
        "entryPoint":"test_alg1.py"
    })
    job = alg.connectToWorker(socket)
    job.join()

if __name__ == '__main__':
    main_callbacks()
