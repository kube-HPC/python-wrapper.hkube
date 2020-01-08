import os
import sys
import logging
import signal
import time
from threading import Thread
import msgpack
from hkube_python_wrapper import Algorunner, HKubeApi


socket = {
    "port": os.environ.get('WORKER_SOCKET_PORT', "3000"),
    "host": os.environ.get('WORKER_SOCKET_HOST', "127.0.0.1"),
    "protocol": os.environ.get('WORKER_SOCKET_PROTOCOL', "ws"),
    "url": os.environ.get('WORKER_SOCKET_URL', None),
}


def start(args, hkubeApi=None):
    logging.info('start called')
    threads = []
    num_threads=1
    results = [{} for x in range(num_threads)]
    def algo(index):
        results[index] = hkubeApi.start_algorithm('eval-alg', [bytearray(b'\xfa'*2000), 6], resultAsRaw=True)

    for ii in range(num_threads):
        p=Thread(target=algo, args=[ii], name=str(ii+1) )
        p.start()
        threads.append(p)
    for process in threads:
        process.join()

    # ret = hkubeApi.start_algorithm('eval-alg', [5, 6], resultAsRaw=True)
    ret2 = hkubeApi.start_stored_subpipeline('simple', {'d': [6, 'stam']})
    # ret = waiter1.get()
    # res = [waiter1.get(), waiter2.get()]
    # print('got all results')
    # ret = list(map(lambda x: {'error': x.get('error')} if x.get(
    #     'error') != None else {'response': x.get('response')}, res))
    # ret='OK!!!'
    print(type(results[0]['response']))
    print(type(bytearray(results[0]['response'])))
    return (results,ret2)


def signal_handler_function(signum, frame):
    print('Got signal', signum )
    sys.exit(0)

def main_callbacks():
    logging.basicConfig(level=0,format='%(asctime)s %(threadName)s %(message)s', stream=sys.stdout)
    signal.signal(signal.SIGTERM, signal_handler_function)
    signal.signal(signal.SIGINT, signal_handler_function)
    alg = Algorunner()
    alg.loadAlgorithmCallbacks(start)
    job = alg.connectToWorker(socket)
    alg.run()
    logging.info('Done!!!!!')

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
