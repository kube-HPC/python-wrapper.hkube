import logging
import time
from jaeger_client import Config
import opentracing


class Tracer(object):
    instance = None

    def __init__(self, tracer_config):
        if (Tracer.instance is not None and Tracer.instance is not None):
            Tracer.instance.close()
        defaultConfig = {  # usually read from some yaml config
            'sampler': {
                'type': 'const',
                'param': 1,
            },
            'logging': False,
        }
        config = tracer_config.get(
            'config', defaultConfig) if tracer_config is not None else defaultConfig
        service_name = tracer_config.get(
            'service_name', 'algorunner') if tracer_config is not None else 'algorunner'
        tracerConfig = Config(
            config=config,
            service_name=service_name,
            validate=True
        )
        self.tracer = tracerConfig.new_tracer()
        opentracing.set_global_tracer(self.tracer)
        Tracer.instance = self

    def close(self):
        if (self.tracer is not None):
            self.tracer.close()

    def extract(self, data):
        return self.tracer.extract(opentracing.Format.TEXT_MAP, data)
    
    def inject(self, span_context, carrier):
        return self.tracer.inject(span_context,opentracing.Format.TEXT_MAP,carrier)
