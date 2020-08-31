import pytest
from hkube_python_wrapper.tracing import Tracer
from tests.configs import config
import logging
import time
import requests
import random
import string

def randomString(stringLength=8):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


def get_jaeger_trace(service_name='algorithm', operation='',limit=1):
    params={
        'service':service_name,
        'operation':operation,
        'limit':limit
        }
    r = requests.get('http://localhost:16686/api/traces',params)
    return r.json()


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
    op1=randomString()
    with tracer.tracer.start_span(operation_name=op1) as span1:
        assert span1 is not None
    time.sleep(2)
    tracer.close()
    r=get_jaeger_trace(operation=op1)
    assert r['data'][0]
        
def test_tracer_start_child_span():
    tracer = Tracer(config.tracer)
    op1=randomString()
    op2=randomString()
    with tracer.tracer.start_active_span(operation_name=op1) as span1:
        with tracer.tracer.start_active_span(operation_name=op2) as span2:
            assert span1.span is not None
            assert span2.span is not None
            assert span2.span.parent_id is span1.span.span_id
    time.sleep(2)
    tracer.close()
    r=get_jaeger_trace(operation=op1)
    assert r['data'][0]
    op1Trace=r['data'][0]['traceID']
    r=get_jaeger_trace(operation=op2)
    assert r['data'][0]
    op2Trace=r['data'][0]['traceID']
    assert op1Trace==op2Trace

