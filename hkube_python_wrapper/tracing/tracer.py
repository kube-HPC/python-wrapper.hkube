from jaeger_client import Config
import opentracing


class Tracer(object):
    instance = None

    def __init__(self, tracer_config):
        if (Tracer.instance is not None):
            Tracer.instance.close()

        config = tracer_config.get('config')
        service_name = tracer_config.get('service_name')
        tracerConfig = Config(
            config=config,
            service_name=service_name,
            validate=True
        )
        self.tracer = tracerConfig.new_tracer()
        Tracer.instance = self

    def close(self):
        if (self.tracer is not None):
            self.tracer.close()

    def extract(self, data):
        return self.tracer.extract(opentracing.Format.TEXT_MAP, data)

    def inject(self, span_context, carrier):
        return self.tracer.inject(span_context, opentracing.Format.TEXT_MAP, carrier)

    def create_span(self, operation_name, parent_span, jobId, taskId, nodeName):
        child_of = Tracer.instance.extract(
            parent_span) if parent_span else None
        span = self.tracer.start_active_span(
            operation_name=operation_name, child_of=child_of)
        span.span.set_tag('jobId', jobId)
        span.span.set_tag('taskId', taskId)
        span.span.set_tag('nodeName', nodeName)
        return span

    def finish_span(self, span, error=None):
        if (span is None):
            return
        if (error is not None):
            span.span.set_tag('error', "true")
            span.span.log_kv({'event': 'error', 'error.object': error})
        span.span.finish()
