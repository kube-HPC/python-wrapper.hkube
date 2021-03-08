from __future__ import print_function, division, absolute_import


class Job:
    def __init__(self, options):
        self._options = options

    @property
    def jobId(self):
        return self._options.get('jobId')

    @property
    def taskId(self):
        return self._options.get('taskId')

    @property
    def nodeName(self):
        return self._options.get('nodeName')

    @property
    def childs(self):
        return self._options.get('childs')

    @property
    def parsedFlow(self):
        return self._options.get('parsedFlow')

    @property
    def defaultFlow(self):
        return self._options.get('defaultFlow')

    @property
    def info(self):
        return self._options.get('info')

    @property
    def input(self):
        return self._options.get('input')

    @property
    def flatInput(self):
        return self._options.get('flatInput')

    @property
    def storage(self):
        return self._options.get('storage')

    @property
    def spanId(self):
        return self._options.get('spanId')

    @property
    def isStreaming(self):
        return self._options.get('kind') == 'stream'

    @property
    def isStateful(self):
        return self._options.get('stateType') != 'stateless'
