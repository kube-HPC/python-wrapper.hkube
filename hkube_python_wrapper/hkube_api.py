from __future__ import print_function, division, absolute_import
from .execution import AlgorithmExecution
from .waitFor import WaitForData
import hkube_python_wrapper.messages as messages
import threading
from events import Events
import sys
import os
import gevent
from gevent import monkey
monkey.patch_all()


class HKubeApi:
    def __init__(self, wc):
        self._wc = wc
        self._algorithmExecutionsMap = {}
        self._lastExecId = 0
        self._wc.events.on_algorithmExecutionDone += self.algorithmExecutionDone
        self._wc.events.on_algorithmExecutionError += self.algorithmExecutionDone

        self._wc.events.on_subPipelineDone += self.subPipelineDone
        self._wc.events.on_subPipelineError += self.subPipelineDone
        self._wc.events.on_subPipelineStopped += self.subPipelineDone



    def algorithmExecutionDone(self, data):
        execution = self._algorithmExecutionsMap.get(int(data.get('execId')))
        execution.waiter.set(data)

    def subPipelineDone(self, data):
        print('subPipelineDone', data)
        execution = self._algorithmExecutionsMap.get(int(data.get('subPipelineId')))
        execution.waiter.set(data)

    def start_algorithm(self, algorithmName, input=[], resultAsRaw=False, blocking=False):
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

        if blocking:
            return execution.waiter.get()
        return execution.waiter

    def start_stored_subpipeline(self, name, flowInput={}, blocking=False):
        print('start_stored_subpipeline called with {name}'.format(name=name))
        self._lastExecId += 1
        execution = AlgorithmExecution(self._lastExecId, WaitForData(True))
        self._algorithmExecutionsMap[self._lastExecId] = execution

        message = {
            "command": messages.outgoing["startStoredSubPipeline"],
            "data": {
                "subPipeline": {
                    "name": name,
                    "flowInput": flowInput
                },
                "subPipelineId": self._lastExecId,

            }
        }
        self._wc.send(message)

        if blocking:
            return execution.waiter.get()
        return execution.waiter

    def start_raw_subpipeline(self, name, nodes, flowInput, options=None, webhooks=None, blocking=False):
        print('start_raw_subpipeline called with {name}'.format(name=name))
        self._lastExecId += 1
        execution = AlgorithmExecution(self._lastExecId, WaitForData(True))
        self._algorithmExecutionsMap[self._lastExecId] = execution

        message = {
            "command": messages.outgoing["startRawSubPipeline"],
            "data": {
                "subPipeline": {
                    "name": name,
                    "nodes": nodes,
                    "options": options,
                    "webhooks": webhooks,
                    "flowInput": flowInput
                },
                "subPipelineId": self._lastExecId,

            }
        }
        self._wc.send(message)

        if blocking:
            return execution.waiter.get()
        return execution.waiter
