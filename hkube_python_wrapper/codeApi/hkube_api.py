from __future__ import print_function, division, absolute_import
from hkube_python_wrapper.wrapper.messages import messages
from .execution import AlgorithmExecution
from .waitFor import WaitForData


class HKubeApi:
    def __init__(self, wc, dataAdapter):
        self._wc = wc
        self._dataAdapter = dataAdapter
        self._algorithmExecutionsMap = {}
        self._lastExecId = 0
        self._wc.events.on_algorithmExecutionDone += self.algorithmExecutionDone
        self._wc.events.on_algorithmExecutionError += self.algorithmExecutionDone

        self._wc.events.on_subPipelineDone += self.subPipelineDone
        self._wc.events.on_subPipelineError += self.subPipelineDone
        self._wc.events.on_subPipelineStopped += self.subPipelineDone

    def algorithmExecutionDone(self, data):
        response = data.get('response')
        execId = data.get('execId')
        result = data
        if(response):
            result = self._dataAdapter.tryGetDataFromPeerOrStorage(response)

        execution = self._algorithmExecutionsMap.get(execId)
        execution.waiter.set(result)

    def subPipelineDone(self, data):
        response = data.get('response')
        subPipelineId = data.get('subPipelineId')
        result = data
        if(response):
            result = self._dataAdapter.tryGetDataFromPeerOrStorage(response)

        execution = self._algorithmExecutionsMap.get(subPipelineId)
        execution.waiter.set(result)

    def start_algorithm(self, algorithmName, input=[], resultAsRaw=False, blocking=False):
        print('start_algorithm called with {name}'.format(name=algorithmName))
        self._lastExecId += 1
        execId = str(self._lastExecId)
        execution = AlgorithmExecution(execId, WaitForData(True))
        self._algorithmExecutionsMap[execId] = execution

        message = {
            "command": messages.outgoing.startAlgorithmExecution,
            "data": {
                "execId": execId,
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
        execId = str(self._lastExecId)
        execution = AlgorithmExecution(execId, WaitForData(True))
        self._algorithmExecutionsMap[execId] = execution

        message = {
            "command": messages.outgoing.startStoredSubPipeline,
            "data": {
                "subPipeline": {
                    "name": name,
                    "flowInput": flowInput
                },
                "subPipelineId": execId
            }
        }
        self._wc.send(message)

        if blocking:
            return execution.waiter.get()
        return execution.waiter

    def start_raw_subpipeline(self, name, nodes, flowInput, options=None, webhooks=None, blocking=False):
        print('start_raw_subpipeline called with {name}'.format(name=name))
        self._lastExecId += 1
        execId = str(self._lastExecId)
        execution = AlgorithmExecution(execId, WaitForData(True))
        self._algorithmExecutionsMap[execId] = execution

        message = {
            "command": messages.outgoing.startRawSubPipeline,
            "data": {
                "subPipeline": {
                    "name": name,
                    "nodes": nodes,
                    "options": options,
                    "webhooks": webhooks,
                    "flowInput": flowInput
                },
                "subPipelineId": execId
            }
        }
        self._wc.send(message)

        if blocking:
            return execution.waiter.get()
        return execution.waiter
