import gevent
import copy


class statelessALgoWrapper(object):
    def __init__(self, algo):
        self._hkubeApi = None
        self.algo = algo
        self.options = None
        self.active = True

    def _invokeAlgorithm(self, msg, origin):
        if not (self.algo['init'] is None):
            self.algo['init'](msg)
        input = copy.copy(self.options['input'])
        input.append({origin: msg})
        result = self.algo['start'](self.options, self._hkubeApi)
        self._hkubeApi.sendMessage(result)

    def start(self, options, hkube_api):
        # pylint: disable=unused-argument
        self._hkubeApi = hkube_api
        self._hkubeApi.registerInputListener(onMessage=self._invokeAlgorithm)
        self._hkubeApi.startMessageListening()
        while (self.active):
            gevent.sleep(1)

    def init(self, options):
        self.options = options

    def exit(self, data):
        if not (self.algo.get('exit') is None):
            try:
                self.algo['exit'](data)
            except Exception as e:
                print('Failure during algorithm exit ' + e)
        self.active = False

    def stop(self, data):
        if not (self.algo.get('stop') is None):
            try:
                self.algo['stop'](data)
            except Exception as e:
                print('Failure during algorithm stop ' + e)
        self.active = False
