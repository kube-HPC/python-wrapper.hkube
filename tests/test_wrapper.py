

import os
import time
from hkube_python_wrapper import Algorunner
from tests.configs import config
from tests.mocks import mockdata


def startCallback(args):
    return args["input"]["input"][0]


def test_load_algorithm_callbacks():
    algorunner = Algorunner()
    algorunner.loadAlgorithmCallbacks(startCallback)
    result1 = algorunner._algorithm['start']({'input': mockdata.initData}, None)
    result2 = startCallback({'input': mockdata.initData})
    assert result1 == result2


def test_failed_load_algorithm():
    options = {
        "path": "no_such_path",
        "entryPoint": "main.py"
    }
    algorunner = Algorunner()
    algorunner.loadAlgorithm(options)
    assert "No module named" in algorunner._loadAlgorithmError
    assert "no_such_path" in algorunner._loadAlgorithmError


def xtest_load_algorithm():
    options = {
        "path": "test_alg",
        "entryPoint": "main.py"
    }
    cwd = os.getcwd()
    os.chdir(cwd + '/tests')
    algorunner = Algorunner()
    algorunner.loadAlgorithm(options)

    # os.chdir(cwd)
    result1 = algorunner._algorithm['start']({'input': mockdata.initData}, None)
    result2 = startCallback({'input': mockdata.initData})
    assert result1 == result2


def test_connect_to_worker():
    config.discovery.update({"port": "9021"})
    algorunner = Algorunner()
    algorunner.loadAlgorithmCallbacks(startCallback)
    algorunner.connectToWorker(config)
    time.sleep(2)
    assert algorunner._connected == True
    assert algorunner._input == mockdata.initData
