from __future__ import print_function, division, absolute_import
import os
from hkube_python_wrapper import Algorunner

socket = {
    "port": os.environ.get('WORKER_SOCKET_PORT', "3000"),
    "host": os.environ.get('WORKER_SOCKET_HOST', "127.0.0.1"),
    "protocol": os.environ.get('WORKER_SOCKET_PROTOCOL', "ws"),
    "url": os.environ.get('WORKER_SOCKET_URL', None),
}


def start(args, hkubeApi=None):
    print('start called')
    waiter1 = hkubeApi.start_algorithm('eval-alg', [5, 6], includeResult=True)
    waiter2 = hkubeApi.start_stored_subpipeline('simple', {'d': [6, 'stam']})
    res = [waiter1.get(), waiter2.get()]
    print('got all results')
    ret = list(map(lambda x: {'error': x.get('error')} if x.get(
        'error') != None else {'response': x.get('response')}, res))
    # ret='OK!!!'
    return ret


def main():
    alg = Algorunner()
    alg.loadAlgorithmCallbacks(start)
    job = alg.connectToWorker(socket)
    job.join()


if __name__ == "__main__":
    main()
