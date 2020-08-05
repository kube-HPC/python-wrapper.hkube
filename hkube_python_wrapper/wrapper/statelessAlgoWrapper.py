import gevent


class statelessALgoWrapper(object):
    def __init__(self, algo):
        self._hkubeApi = None
        self.algo = algo
        self.options = None

    def _invokeAlgorithm(self, msg):
        if not (self.algo['init'] is None):
            self.algo['init'](msg)
        result = self.algo['start'](msg, self._hkubeApi)
        self._hkubeApi.produce(result)

    def start(self, options, hkube_api):
        # pylint: disable=unused-argument
        self._hkubeApi = hkube_api
        self._hkubeApi.registerInputListener(onMessage=self._invokeAlgorithm)
        self._hkubeApi.startMessageListening()
        while (True):
            gevent.sleep(1)

    def init(self, options):
        self.options = options

    def exit(self, data):
        if not (self.algo.get('exit') is None):
            self.algo['exit'](data)

    def stop(self, data):
        if not (self.algo.get('stop') is None):
            self.algo['stop'](data)
