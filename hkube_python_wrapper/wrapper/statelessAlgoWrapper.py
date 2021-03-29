import time
import threading
from hkube_python_wrapper.util.logger import log

class statelessAlgoWrapper(object):
    def __init__(self, algo):
        self._hkubeApi = None
        self.originalAlgorithm = algo
        self.options = None
        self.active = False
        self.error = None
        self.threadLocalStorage = threading.local()

    def _invokeAlgorithm(self, msg, origin):
        if not (self.originalAlgorithm.get('init') is None):
            self.originalAlgorithm['init'](msg)
            # TODO should init be called upon every message
        options = {}
        options.update(self.options)
        options['streamInput'] = {'message': msg, 'origin': origin}
        try:
            result = self.originalAlgorithm['start'](options, self._hkubeApi)
            if (self.options['childs']):
                self._hkubeApi.sendMessage(result)
        except Exception as e:
            log.error('statelessWrapper error, {e}', e=str(e))
            self.error = e

    def start(self, options, hkube_api):
        # pylint: disable=unused-argument
        self._hkubeApi = hkube_api
        self._hkubeApi.registerInputListener(onMessage=self._invokeAlgorithm)
        self._hkubeApi.startMessageListening()
        self.active = True
        self.error = None
        while (self.active):
            if (self.error is not None):
                raise self.error  # pylint: disable=raising-bad-type
            time.sleep(1)

    def init(self, options):
        self.options = options

    def exit(self, data):
        self.active = False
        if not (self.originalAlgorithm.get('exit') is None):
            self.originalAlgorithm['exit'](data)

    def stop(self, data):
        self.active = False
        if self.originalAlgorithm.get('stop'):
            self.originalAlgorithm['stop'](data)
