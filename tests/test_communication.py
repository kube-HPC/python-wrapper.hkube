
from time import sleep
from hkube_python_wrapper import Algorunner
from tests.configs import config


def test_load_algorithm_callbacks(mocker):
    interval = 500
    config.discovery["servingReportInterval"] = interval
    config.discovery["enable"] = True
    algorunner = Algorunner()
    spy = mocker.spy(algorunner, '_reportServingStatus')
    algorunner._initDataServer(config)
    sleep(0.8)
    assert spy.call_count == 2
    algorunner.close()
    