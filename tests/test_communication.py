
from time import sleep
from hkube_python_wrapper import Algorunner
from hkube_python_wrapper.wrapper.job import Job
from tests.configs import config



def test_load_algorithm_callbacks(mocker):
    interval = 500
    config.discovery["servingReportInterval"] = interval
    config.discovery["enable"] = True
    algorunner = Algorunner()
    algorunner._job = Job({"kind": "batch"})
    spy = mocker.spy(algorunner, '_reportServingStatus')
    algorunner._initDataServer(config)
    sleep(0.8)
    assert spy.call_count == 2
    algorunner.close()
    