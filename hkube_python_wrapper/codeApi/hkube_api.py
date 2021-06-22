from __future__ import print_function, division, absolute_import
import time
from hkube_python_wrapper.util.object_path import getPath
import hkube_python_wrapper.util.type_check as typeCheck
from hkube_python_wrapper.wrapper.messages import messages
from .execution import Execution
from .waitFor import WaitForData
from hkube_python_wrapper.util.queueImpl import Empty
from hkube_python_wrapper.util.logger import log



class HKubeApi:
    """Hkube interface for code-api operations"""

    def __init__(self, wc, wrapper, dataAdapter, storage, streamingManager):
        self._wc = wc
        self._wrapper = wrapper
        self._dataAdapter = dataAdapter
        self._storage = storage
        self._executions = {}
        self._lastExecId = 0
        self.streamingManager = streamingManager
        if self._storage == "v1":
            self.sendMessage = self.sendRemoteStorage

    def registerInputListener(self, onMessage):
        self.streamingManager.registerInputListener(onMessage)

    def startMessageListening(self):
        self.streamingManager.startMessageListening()

    def sendMessage(self, msg, flowName=None): # pylint: disable=method-hidden
        self.streamingManager.sendMessage(msg, flowName)

    def sendRemoteStorage(self, msg, flowName=None):
        message = {
            "command": messages.outgoing.streamingOutMessage,
            "data": {
                "message": msg,
                "flowName": flowName,
                "sendMessageId": self.get_local_sendMessage()
            }
        }
        self._wc.send(message)

    def get_local_sendMessage(self):
        try:
            return self.streamingManager.threadLocalStorage.sendMessageId
        except Exception:
            return None

    def stopStreaming(self, force=True):
        self.streamingManager.stopStreaming(force)

    def isListeningToMessages(self):
        return self.streamingManager.listeningToMessages

    def _generateExecId(self):
        self._lastExecId += 1
        return str(self._lastExecId)

    def algorithmExecutionDone(self, data):
        execId = data.get('execId')
        self._handleExecutionDone(execId, data)

    def subPipelineDone(self, data):
        subPipelineId = data.get('subPipelineId')
        self._handleExecutionDone(subPipelineId, data)

    def dataSourceResponse(self, data):
        requestId = data.get('requestId')
        execution = self._executions.get(requestId)
        try:
            error = data.get('error')
            dataSource = None
            if (not error):
                dataSource = data.get('response')
            execution.waiter.set((error, dataSource))
        except Exception as e:
            execution.waiter.set(e)
        finally:
            self._executions.pop(requestId)

    def _handleExecutionDone(self, execId, data):
        execution = self._executions.get(execId)
        # pylint: disable=too-many-nested-blocks
        try:
            error = data.get('error')
            if (error):
                execution.waiter.set(error)

            elif (execution.includeResult):
                response = data.get('response')
                result = response
                if (typeCheck.isDict(response) and response.get('storageInfo') and self._storage == 'v3'):
                    result = self._dataAdapter.tryGetDataFromPeerOrStorage(
                        response)
                if self._storage == 'v3' and typeCheck.isList(result):
                    for node in result:
                        if typeCheck.isDict(node) and getPath(node, 'info.isBigData') is True:
                            nodeResult = self._dataAdapter.tryGetDataFromPeerOrStorage({"storageInfo": node['info']})
                            node['result'] = nodeResult
                execution.waiter.set(result)
            else:
                execution.waiter.set(None)

        except Exception as e:
            execution.waiter.set(e)
        finally:
            self._executions.pop(execId)

    def start_algorithm(self, algorithmName, input=[], includeResult=True):
        """Starts algorithm execution.

        starts an invocation of algorithm with input, and waits for results

        Args:
            algorithmName (string): The name of the algorithm to start.
            input (array): Optional input for the algorithm.
            includeResult (bool): if True, returns the result of the algorithm execution.
        Returns:
            Returns the result of the algorithm
        """
        log.info('start_algorithm called with {name}', name=algorithmName)
        execId = self._generateExecId()
        execution = Execution(execId, includeResult, WaitForData(True))
        self._executions[execId] = execution
        if self._storage == 'v3':
            (storage, mappedInput) = self._dataAdapter.setAlgorithmStorage(self._wrapper.getCurrentJob().jobId, input)
            message = {
                "command": messages.outgoing.startAlgorithmExecution,
                "data": {
                    "execId": execId,
                    "algorithmName": algorithmName,
                    "storageInput": mappedInput,
                    "storage": storage,
                    "includeResult": includeResult
                }
            }
        else:
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

        return self._waitForResult(execution)

    def getDataSource(self, dataSource):
        log.info('getDataSource called')
        requestId = self._generateExecId()
        execution = Execution(requestId, False, WaitForData(True))
        self._executions[requestId] = execution

        message = {
            "command": messages.outgoing.dataSourceRequest,
            "data": {
                "requestId": requestId,
                "dataSource": dataSource
            }
        }
        self._wc.send(message)

        return self._waitForResult(execution)

    def start_stored_subpipeline(self, name, flowInput={}, includeResult=True):
        """Starts pipeline execution.

        starts an invocation of a sub-pipeline with input, and waits for results

        Args:
            name (string): The name of the pipeline to start.
            flowInput (dict): Optional flowInput for the pipeline.
            includeResult (bool): if True, returns the result of the pipeline execution.
                default: True
        Returns:
            Returns the result of the pipeline
        """
        log.info('start_stored_subpipeline called with {name}', name=name)
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
        return self._waitForResult(execution)

    def start_raw_subpipeline(self, name, nodes, flowInput, options={}, webhooks={}, includeResult=True):
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
        Returns:
            Returns the result of the pipeline
        """
        log.info('start_raw_subpipeline called with {name}', name=name)
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
        return self._waitForResult(execution)

    def _waitForResult(self, execution):
        while not execution.waiter.ready():
            try:
                (command, data) = self._wrapper.get_message(False)
                self._wrapper.handle(command, data)
            except Empty:
                pass
            time.sleep(0.01)
        return execution.waiter.get()
