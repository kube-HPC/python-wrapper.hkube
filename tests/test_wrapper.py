import os
import time
import pytest

# from mock import patch
from time import sleep
from threading import Thread

from hkube_python_wrapper.communication.streaming.StreamingManager import StreamingManager
from hkube_python_wrapper import Algorunner
from tests.configs import config
from tests.mocks import mockdata
from hkube_python_wrapper.codeApi.hkube_api import HKubeApi

oneMB = 1024 * 1024


class Algorithm(object):
    pass


def startCallbackBytes(args):
    return bytearray(b'\xdd' * (1 * oneMB))


def startCallback(args):
    return args["input"]["input"][0]


def test_load_algorithm_callbacks():
    algorunner = Algorunner()
    algorunner.loadAlgorithmCallbacks(startCallback, options=config)
    result1 = algorunner._originalAlgorithm['start']({'input': mockdata.initData}, None)
    result2 = startCallback({'input': mockdata.initData})
    assert result1 == result2
    algorunner.close()


def test_load_algorithm_streaming_then_batch():
    algorunner = Algorunner()
    algorunner.loadAlgorithmCallbacks(startCallback, options=config)
    algorunner.streamingManager = StreamingManager(None)
    algorunner._hkubeApi = HKubeApi(None, algorunner, None, None,algorunner.streamingManager)
    algorunner._init(mockdata.streamingInitData)
    thrd = Thread(target=algorunner._originalAlgorithm['start'], args=[{'input': mockdata.streamingInitData}, algorunner._hkubeApi])
    thrd.start()
    algorunner._stopAlgorithm(mockdata.initData)
    result1 = algorunner._originalAlgorithm['start']({'input': mockdata.initData}, algorunner._hkubeApi)
    result2 = startCallback({'input': mockdata.initData})
    assert result1 == result2
    algorunner.close()


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
        Thread(target=invokeExit).start()
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
    algorunner.close()


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
    result1 = algorunner._originalAlgorithm['start']({'input': mockdata.initData}, None)
    result2 = startCallback({'input': mockdata.initData})
    assert result1 == result2

@pytest.mark.parametrize("test_input,expected", [
    ('main.py','main'),
    ('main','main'),
    ('foo.bar.main.py','foo.bar.main'),
    ('foo.bar.main','foo.bar.main'),
    ('foo/bar/main.py','foo.bar.main'),
    ('foo/bar/main','foo.bar.main'),
    ])
def test_entryPoint(test_input,expected):
    actual = Algorunner._getEntryPoint(test_input)
    assert actual == expected

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
    algorunner.close()
