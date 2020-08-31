import os
import time
# from mock import patch
from gevent import spawn, sleep
from hkube_python_wrapper import Algorunner
from tests.configs import config
from tests.mocks import mockdata

oneMB = 1024 * 1024

class Algorithm(object):
    pass


def startCallbackBytes(args):
    return bytearray(b'\xdd'*(1 * oneMB))


def startCallback(args):
    return args["input"]["input"][0]


def test_load_algorithm_callbacks():
    algorunner = Algorunner()
    algorunner.loadAlgorithmCallbacks(startCallback, options=config)
    result1 = algorunner._algorithm['start']({'input': mockdata.initData}, None)
    result2 = startCallback({'input': mockdata.initData})
    assert result1 == result2


def xtest_exit():
    with patch('sys.exit') as exit_mock:
        def doExit(a):
            status['exit'] = True

        def invokeExit():
            algorunner._exit(None)

        def isServingTrue():
            return True

        def isServingFalse():
            return False
        algorunner = Algorunner()
        algorunner.loadAlgorithmCallbacks(startCallback)
        algorunner.connectToWorker(config)
        sleep(1)
        status = {'exit': False}
        algorunner.loadAlgorithmCallbacks(startCallback, exit=doExit)
        algorunner._dataServer.isServing = isServingTrue
        spawn(invokeExit)
        sleep(1)
        assert status['exit'] == False
        algorunner._dataServer.isServing = isServingFalse
        sleep(1)
        assert status['exit'] == True
        assert exit_mock.called


def test_failed_load_algorithm():
    alg = Algorithm()
    alg.algorithm = {
        "path": "no_such_path",
        "entryPoint": "main.py"
        }
    algorunner = Algorunner()
    algorunner.loadAlgorithm(alg)
    assert "No module named" in algorunner._loadAlgorithmError
    assert "no_such_path" in algorunner._loadAlgorithmError


def xtest_load_algorithm():
    alg = Algorithm()
    alg.algorithm = {
        "path": "test_alg",
        "entryPoint": "main.py"
    }
    cwd = os.getcwd()
    os.chdir(cwd + '/tests')
    algorunner = Algorunner()
    algorunner.loadAlgorithm(alg)

    # os.chdir(cwd)
    result1 = algorunner._algorithm['start']({'input': mockdata.initData}, None)
    result2 = startCallback({'input': mockdata.initData})
    assert result1 == result2

def startCallback2(args):
    return args["input"][0]

def test_connect_to_worker():
    config.discovery.update({"port": "9021"})
    algorunner = Algorunner()
    algorunner.loadAlgorithmCallbacks(startCallback2, options=config)
    algorunner.connectToWorker(config)
    time.sleep(2)
    assert algorunner._connected == True
    assert algorunner._input == mockdata.initData
