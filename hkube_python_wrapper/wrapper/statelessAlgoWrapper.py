import gevent
import copy


class statelessALgoWrapper(object):
    def __init__(self, algo):
        self._hkubeApi = None
        self.algo = algo
        self.options = None
        self.active = True
        self.error = None

    def _invokeAlgorithm(self, msg, origin):
        if not (self.algo['init'] is None):
            self.algo['init'](msg)
        input = copy.copy(self.options['input'])
        input.append({origin: msg})
        try:
            result = self.algo['start'](self.options, self._hkubeApi)
            self._hkubeApi.sendMessage(result)
        except Exception as e:
            self.error = e



    def start(self, options, hkube_api):
        # pylint: disable=unused-argument
        self._hkubeApi = hkube_api
        self._hkubeApi.registerInputListener(onMessage=self._invokeAlgorithm)
        self._hkubeApi.startMessageListening()
        while (self.active):
            if(self.error is not None):
                raise self.error  # pylint: disable=raising-bad-type
            gevent.sleep(1)

    def init(self, options):
        self.options = options

    def exit(self, data):
        self.active = False
        if not (self.algo.get('exit') is None):
            self.algo['exit'](data)



    def stop(self, data):
        self.active = False
        if not (self.algo.get('stop') is None):
            self.algo['stop'](data)
