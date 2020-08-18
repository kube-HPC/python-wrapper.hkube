import gevent
import copy


class statelessALgoWrapper(object):
    def __init__(self, algo):
        self._hkubeApi = None
        self.originalAlgorithm = algo
        self.options = None
        self.active = True
        self.error = None

    def _invokeAlgorithm(self, msg, origin):
        if not (self.originalAlgorithm['init'] is None):
            self.originalAlgorithm['init'](msg)
        input = copy.copy(self.options['input'])

        for index, item in enumerate(input):
            if (item == '@' + origin):
                input[index] = msg
        options = {}
        options.update(self.options)
        options['input'] = input
        try:
            result = self.originalAlgorithm['start'](self.options, self._hkubeApi)
            self._hkubeApi.sendMessage(result)
        except Exception as e:
            self.error = e

    def start(self, options, hkube_api):
        # pylint: disable=unused-argument
        self._hkubeApi = hkube_api
        self._hkubeApi.registerInputListener(onMessage=self._invokeAlgorithm)
        self._hkubeApi.startMessageListening()
        while (self.active):
            if (self.error is not None):
                raise self.error  # pylint: disable=raising-bad-type
            gevent.sleep(1)

    def init(self, options):
        self.options = options

    def exit(self, data):
        self.active = False
        if not (self.originalAlgorithm.get('exit') is None):
            self.originalAlgorithm['exit'](data)

    def stop(self, data):
        self.active = False
        if not (self.originalAlgorithm.get('stop') is None):
            self.originalAlgorithm['stop'](data)
