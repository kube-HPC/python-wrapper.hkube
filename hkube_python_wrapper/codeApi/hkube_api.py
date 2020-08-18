from __future__ import print_function, division, absolute_import
import hkube_python_wrapper.util.type_check as typeCheck
from hkube_python_wrapper.wrapper.messages import messages
from .execution import Execution
from .waitFor import WaitForData


class HKubeApi:
    """Hkube interface for code-api operations"""
    def __init__(self, wc, dataAdapter, storage):
        self._wc = wc
        self._dataAdapter = dataAdapter
        self._storage = storage
        self._executions = {}
        self._lastExecId = 0
        self._wc.events.on_algorithmExecutionDone += self.algorithmExecutionDone
        self._wc.events.on_algorithmExecutionError += self.algorithmExecutionDone

        self._wc.events.on_subPipelineDone += self.subPipelineDone
        self._wc.events.on_subPipelineError += self.subPipelineDone
        self._wc.events.on_subPipelineStopped += self.subPipelineDone

    def _generateExecId(self):
        self._lastExecId += 1
        return str(self._lastExecId)

    def algorithmExecutionDone(self, data):
        execId = data.get('execId')
        self._handleExecutionDone(execId, data)

    def subPipelineDone(self, data):
        subPipelineId = data.get('subPipelineId')
        self._handleExecutionDone(subPipelineId, data)

    def _handleExecutionDone(self, execId, data):
        execution = self._executions.get(execId)

        try:
            error = data.get('error')
            if(error):
                execution.waiter.set(error)

            elif(execution.includeResult):
                response = data.get('response')
                result = response
                if(typeCheck.isDict(response) and response.get('storageInfo') and self._storage == 'v2'):
                    result = self._dataAdapter.tryGetDataFromPeerOrStorage(response)
                execution.waiter.set(result)
            else:
                execution.waiter.set(None)

        except Exception as e:
            execution.waiter.set(e)
        finally:
            self._executions.pop(execId)

    def start_algorithm(self, algorithmName, input=[], includeResult=True, blocking=False):
        """Starts algorithm execution.

    starts an invocation of algorithm with input, and optionally waits for results

    Args:
        algorithmName (string): The name of the algorithm to start.
        input (array): Optional input for the algorithm.
        includeResult (bool): if True, returns the result of the algorithm execution.
        blocking (bool): if True, blocks until the algorithm finises, and returns the results.
            If False, returns an awaiter object, that can be awaited (blocking) at a later time
    Returns:
        if blocking==False, returns an awaiter. If true, returns the result of the algorithm
    """
        print('start_algorithm called with {name}'.format(name=algorithmName))
        execId = self._generateExecId()
        execution = Execution(execId, includeResult, WaitForData(True))
        self._executions[execId] = execution

        message = {
            "command": messages.outgoing.startAlgorithmExecution,
            "data": {
                "execId": execId,
                "algorithmName": algorithmName,
                "input": input,
                "includeResult": includeResult
            }
        }
        self._wc.send(message)

        if blocking:
            return execution.waiter.get()
        return execution.waiter

    def start_stored_subpipeline(self, name, flowInput={}, includeResult=True, blocking=False):
        """Starts pipeline execution.

    starts an invocation of a sub-pipeline with input, and optionally waits for results

    Args:
        name (string): The name of the pipeline to start.
        flowInput (dict): Optional flowInput for the pipeline.
        includeResult (bool): if True, returns the result of the pipeline execution.
            default: True
        blocking (bool): if True, blocks until the pipeline finises, and returns the results.
            If False, returns an awaiter object, that can be awaited (blocking) at a later time
            default: False
    Returns:
        if blocking==False, returns an awaiter. If true, returns the result of the pipeline
    """
        print('start_stored_subpipeline called with {name}'.format(name=name))
        execId = self._generateExecId()
        execution = Execution(execId, includeResult, WaitForData(True))
        self._executions[execId] = execution

        message = {
            "command": messages.outgoing.startStoredSubPipeline,
            "data": {
                "subPipeline": {
                    "name": name,
                    "flowInput": flowInput
                },
                "subPipelineId": execId,
                "includeResult": includeResult
            }
        }
        self._wc.send(message)

        if blocking:
            return execution.waiter.get()
        return execution.waiter

    def start_raw_subpipeline(self, name, nodes, flowInput, options={}, webhooks={}, includeResult=True, blocking=False):
        """Starts pipeline execution.

    starts an invocation of a sub-pipeline with input, nodes, options, and optionally waits for results

    Args:
        name (string): The name of the pipeline to start.
        nodes (array): array of node definitions (like in the pipeline descriptor)
        options (dict): pipeline options (like in the pipeline descriptor)
        webhooks (dict): webhook options (like in the pipeline descriptor)
        flowInput (dict): flowInput for the pipeline.
        includeResult (bool): if True, returns the result of the pipeline execution.
            default: True
        blocking (bool): if True, blocks until the pipeline finises, and returns the results.
            If False, returns an awaiter object, that can be awaited (blocking) at a later time
            default: False
    Returns:
        if blocking==False, returns an awaiter. If true, returns the result of the pipeline
    """
        print('start_raw_subpipeline called with {name}'.format(name=name))
        execId = self._generateExecId()
        execution = Execution(execId, includeResult, WaitForData(True))
        self._executions[execId] = execution

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
                "subPipelineId": execId,
                "includeResult": includeResult
            }
        }
        self._wc.send(message)

        if blocking:
            return execution.waiter.get()
        return execution.waiter
