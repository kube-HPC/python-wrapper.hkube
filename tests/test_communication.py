import os
import time
# from mock import patch
from gevent import spawn, sleep
from hkube_python_wrapper import Algorunner
from hkube_python_wrapper.config import config
from tests.mocks import mockdata

def startCallback(args):
    return args["input"]["input"][0]


def xtest_load_algorithm_callbacks():
    config.discovery["servingReportInterval"] = 0.5
    algorunner = Algorunner()
    algorunner.Run(start=startCallback, options=config)
    # algorunner._algorithm['start']({'input': mockdata.initData}, None)
    sleep(3000)
    assert result1 == result2

