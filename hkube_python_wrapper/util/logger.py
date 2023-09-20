import logging
import traceback
from logging import LoggerAdapter

from hkube_python_wrapper.config import config
from hkube_python_wrapper.util.type_check import isString


class Adapter(LoggerAdapter):
    def process(self, msg, kwargs):
        if isString(msg) and kwargs:
            return msg.format(**kwargs), {}
        return msg, kwargs

    def exception(self, err, *args, exc_info=True, **kwargs):
        if self.isEnabledFor(logging.ERROR):
            exc_text = self._format_exception(exc_info)
            self.error('\n' + str(err) + ' ' + exc_text, *args, **kwargs)

    def _format_exception(self, exc_info):
        if exc_info:
            stack_trace = traceback.format_exc().strip()
            return stack_trace.replace('\n', '\\n')
        return ''


log = Adapter(logging.getLogger('wrapper'), {})
algorithmLogger = Adapter(logging.getLogger('algorithm'), {})


def setup():
    level = config.logging.get('level', 'INFO')
    level = level.upper()
    logger = logging.getLogger('wrapper')
    logger.propagate = False
    logger.setLevel(level)
    # create console handler and set level
    ch = logging.StreamHandler()
    ch.setLevel(level)
    # create formatter
    formatter = logging.Formatter('%(name)s::%(levelname)s::%(threadName)s::%(message)s')
    # add formatter to ch
    ch.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(ch)
