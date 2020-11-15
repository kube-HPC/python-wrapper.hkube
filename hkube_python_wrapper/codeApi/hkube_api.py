from __future__ import print_function, division, absolute_import
import time
import hkube_python_wrapper.util.type_check as typeCheck
from hkube_python_wrapper.wrapper.messages import messages
from .execution import Execution
from .waitFor import WaitForData
from hkube_python_wrapper.util.queueImpl import Empty
from ..communication.streaming.MessageListener import MessageListener
from ..communication.streaming.MessageProducer import MessageProducer


class HKubeApi:
    """Hkube interface for code-api operations"""

    def __init__(self, wc, wrapper, dataAdapter, storage):
        self._wc = wc
        self._wrapper = wrapper
        self._dataAdapter = dataAdapter
        self._storage = storage
        self._executions = {}
        self._lastExecId = 0
        self.messageProducer = None
        self._messageListeners = dict()
        self._inputListener = []
        self.listeningToMessages = False

    def setupStreamingProducer(self, onStatistics, producerConfig, nextNodes):
        self.messageProducer = MessageProducer(producerConfig, nextNodes)
        self.messageProducer.registerStatisticsListener(onStatistics)
        if (nextNodes):
            self.messageProducer.start()

    def sendError(self, e):
        self._wrapper.sendError(e)

    def setupStreamingListeners(self, listenerConfig, parents, nodeName):
        print("parents" + str(parents))
        for predecessor in parents:
            remoteAddress = 'tcp://' + \
                            predecessor['address']['host'] + ':' + \
                            str(predecessor['address']['port'])
            if (predecessor['type'] == 'Add'):
                options = {}
                options.update(listenerConfig)
                options['remoteAddress'] = remoteAddress
                options['messageOriginNodeName'] = predecessor['nodeName']
                listener = MessageListener(options, nodeName, self)
                listener.registerMessageListener(self._onMessage)
                self._messageListeners[remoteAddress] = listener
                if (self.listeningToMessages):
                    listener.start()
            if (predecessor['type'] == 'Del'):
                if (self.listeningToMessages):
                    self._messageListeners[remoteAddress].close()
                del self._messageListeners[remoteAddress]

    def registerInputListener(self, onMessage):
        self._inputListener.append(onMessage)

    def _onMessage(self, msg, origin):
        for listener in self._inputListener:
            try:
                listener(msg, origin)
            except Exception as e:
                print("hkube_api message listener through exception: " + str(e))

    def startMessageListening(self):
        self.listeningToMessages = True
        for listener in self._messageListeners.values():
            if not (listener.is_alive()):
                listener.start()

    def sendMessage(self, msg):
        if (self.messageProducer is None):
            raise Exception('Trying to send a message from a none stream pipeline or after close had been sent to algorithm')
        if (self.messageProducer.nodeNames):
            self.messageProducer.produce(msg)

    def stopStreaming(self):
        if (self.listeningToMessages):
            for listener in self._messageListeners.values():
                listener.close()
            self._messageListeners = dict()
        self.listeningToMessages = False
        self._inputListener = []
        if (self.messageProducer is not None):
            self.messageProducer.close()
            self.messageProducer = None

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
            if (error):
                execution.waiter.set(error)

            elif (execution.includeResult):
                response = data.get('response')
                result = response
                if (typeCheck.isDict(response) and response.get('storageInfo') and self._storage == 'v3'):
                    result = self._dataAdapter.tryGetDataFromPeerOrStorage(
                        response)
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
