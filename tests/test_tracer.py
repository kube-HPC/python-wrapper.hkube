import pytest
from hkube_python_wrapper.tracing import Tracer
from tests.configs import config
import logging
import time

log_level = logging.DEBUG
logging.getLogger('').handlers = []
logging.basicConfig(format='%(asctime)s %(message)s', level=log_level)

def test_tracer_fail_init():
    with pytest.raises(TypeError):
        tracer = Tracer()

def test_tracer_init_empty_config():
    with pytest.raises(AttributeError):
        tracer = Tracer(None)

def test_tracer_init_config():
    tracer = Tracer(config.tracer)
    assert tracer.tracer is not None
    assert tracer.tracer.service_name == config.tracer.get("service_name")
    tracer.close()

def test_tracer_start_span():
    tracer = Tracer(config.tracer)
    with tracer.tracer.start_span(operation_name='test1') as span1:
        assert span1 is not None
    tracer.close()
        
def test_tracer_start_child_span():
    tracer = Tracer(config.tracer)
    with tracer.tracer.start_active_span(operation_name='test1') as span1:
        with tracer.tracer.start_active_span(operation_name='test2') as span2:
            assert span1.span is not None
            assert span2.span is not None
            assert span2.span.parent_id is span1.span.span_id
    time.sleep(2)
    tracer.close()