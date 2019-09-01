import os
from websocket_server import WebsocketServer
import simplejson as json
import sys

from hkube_python_wrapper import Algorunner

socket = {
        "port": os.environ.get('WORKER_SOCKET_PORT', "3000"),
        "host": os.environ.get('WORKER_SOCKET_HOST', "127.0.0.1"),
        "protocol": os.environ.get('WORKER_SOCKET_PROTOCOL', "ws"),
        "url": os.environ.get('WORKER_SOCKET_URL', None),
    }

server=None
alg=None

def start(args, hkube=None):
    print('start called')
    return 3

def message_received(client,server, message):
    decoded = json.loads(message)
    command = decoded["command"]
    if command == 'initialized':
        message={'command':'start'}
        server.send_message(client,json.dumps(message))
    if command == 'done':
        print('done')


def connected(client, server):
    message={'command':'initialize', 'data':{'input':[3]}}
    server.send_message(client,json.dumps(message))

def main():
    global server
    global alg
    server = WebsocketServer(int(socket['port']))
    server.set_fn_message_received(message_received)
    server.set_fn_new_client(connected)

    alg = Algorunner()
    alg.loadAlgorithmCallbacks(start)
    alg.connectToWorker(socket)
    server.serve_forever()
    
if __name__ == '__main__':
    main()