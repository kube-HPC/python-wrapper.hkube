import logging
from hkube_python_wrapper.util.type_check import isString
from logging import LoggerAdapter
from hkube_python_wrapper.config import config


class Adapter(LoggerAdapter):
    def process(self, msg, kwargs):
        if isString(msg):
            return msg.format(**kwargs), {}
        return msg, kwargs


log = Adapter(logging.getLogger('wrapper'), {})


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
