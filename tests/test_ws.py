
import time
from hkube_python_wrapper import Algorunner
from storage.storage_manager import StorageManager
from .mock_ws_server import startWebSocketServer, initData
import tests.configs.config as conf
config = conf.Config

returnValue = {"data": "bla"}


def start(args):
    returnValue.update({"output": args["input"]})
    return returnValue


startWebSocketServer(config.socket)

algorunner = Algorunner()
algorunner.loadAlgorithmCallbacks(start)
algorunner.connectToWorker(config)
# gevent.joinall([job1, job2])
time.sleep(2)


def test_get_data():
    data = algorunner._dataServer.data
    assert data['output'] == initData['input']
