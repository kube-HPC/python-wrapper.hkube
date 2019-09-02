from hkube_python_wrapper import Algorunner, HKubeApi
from gevent import monkey
monkey.patch_all()
import sys
import simplejson as json
import os


socket = {
    "port": os.environ.get('WORKER_SOCKET_PORT', "5555"),
    "host": os.environ.get('WORKER_SOCKET_HOST', "127.0.0.1"),
    "protocol": os.environ.get('WORKER_SOCKET_PROTOCOL', "ws"),
    "url": os.environ.get('WORKER_SOCKET_URL', None),
}


def start(args, hkubeApi=None):
    print('start called')
    waiter = hkubeApi.start_algorithm('eval-alg', [5, 6], resultAsRaw=True)
    waiter2 = hkubeApi.start_algorithm(
        'green-alg', [6, 'stam'], resultAsRaw=True)
    ret = [waiter.get().get('response'), waiter2.get().get('response')]
    return ret


def main():
    alg = Algorunner()
    alg.loadAlgorithmCallbacks(start)
    job = alg.connectToWorker(socket)
    job.join()


if __name__ == '__main__':
    main()
