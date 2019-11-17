import os
import simplejson as json
import sys
import logging
from hkube_python_wrapper import Algorunner, HKubeApi


socket = {
    "port": os.environ.get('WORKER_SOCKET_PORT', "3000"),
    "host": os.environ.get('WORKER_SOCKET_HOST', "127.0.0.1"),
    "protocol": os.environ.get('WORKER_SOCKET_PROTOCOL', "ws"),
    "url": os.environ.get('WORKER_SOCKET_URL', None),
}


def start(args, hkubeApi=None):
    logging.info('start called')
    ret = hkubeApi.start_algorithm('eval-alg', [5, 6], resultAsRaw=True)
    ret2 = hkubeApi.start_stored_subpipeline('simple', {'d': [6, 'stam']})
    # ret = waiter1.get()
    # res = [waiter1.get(), waiter2.get()]
    # print('got all results')
    # ret = list(map(lambda x: {'error': x.get('error')} if x.get(
    #     'error') != None else {'response': x.get('response')}, res))
    # ret='OK!!!'
    return (ret,ret2)



def main_callbacks():
    logging.basicConfig(level=0,format='%(relativeCreated)6d %(threadName)s %(message)s', stream=sys.stdout)
    
    alg = Algorunner()
    alg.loadAlgorithmCallbacks(start)
    job = alg.connectToWorker(socket)
    alg.run()
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
