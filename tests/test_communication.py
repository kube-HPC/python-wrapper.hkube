
from gevent import sleep
from hkube_python_wrapper import Algorunner
from hkube_python_wrapper.config import config


def test_load_algorithm_callbacks(mocker):
    interval = 500
    config.discovery["servingReportInterval"] = interval
    algorunner = Algorunner()
    spy = mocker.spy(algorunner, '_reportServingStatus')
    algorunner.connectToWorker(config)
    sleep(0.8)
    assert spy.call_count == 2
    