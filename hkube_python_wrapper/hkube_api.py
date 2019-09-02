from __future__ import print_function, division, absolute_import
import gevent
from gevent import monkey
monkey.patch_all()
import os
import sys
from events import Events
import threading
import hkube_python_wrapper.messages as messages
from .waitFor import WaitForData
from .execution import AlgorithmExecution


class HKubeApi:
    def __init__(self, wc):
        self._wc = wc
        self._algorithmExecutionsMap = {}
        self._lastExecId = 0
        self._wc.events.on_algorithmExecutionDone += self.algorithmExecutionDone

    def algorithmExecutionDone(self, data):
        execution = self._algorithmExecutionsMap.get(int(data.get('execId')))
        execution.waiter.set(data)

    def start_algorithm(self, algorithmName, input=[], resultAsRaw=False):
        print('start_algorithm called with {name}'.format(name=algorithmName))
        self._lastExecId += 1
        execution = AlgorithmExecution(self._lastExecId, WaitForData(True))
        self._algorithmExecutionsMap[self._lastExecId] = execution

        message = {
            "command": messages.outgoing["startAlgorithmExecution"],
            "data": {
                "execId": self._lastExecId,
                "algorithmName": algorithmName,
                "input": input,
                "resultAsRaw": resultAsRaw
            }
        }
        self._wc.send(message)

        # ret = execution.waiter.get()
        return execution.waiter
        # return ret
