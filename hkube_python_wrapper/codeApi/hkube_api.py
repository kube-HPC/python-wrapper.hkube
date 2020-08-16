from __future__ import print_function, division, absolute_import

import gevent

import hkube_python_wrapper.util.type_check as typeCheck
from hkube_python_wrapper.wrapper.messages import messages
from .execution import Execution
from .waitFor import WaitForData
from ..communication.streaming.MessageListener import MessageListener
from ..communication.streaming.MessageProducer import MessageProducer


class HKubeApi:
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
        self._messageProducer = None
        self._messageListeners = dict()
        self._inputListener = []
        self.listeningToMessages = False

    def setupStreamingProducer(self, onStatistics, producerConfig, nextNodes):
        self._messageProducer = MessageProducer(producerConfig, nextNodes)
        self._messageProducer.registerStatisticsListener(onStatistics)
        if (nextNodes):
            gevent.spawn(self._messageProducer.start)

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
                listenr = MessageListener(options, nodeName)
                listenr.registerMessageListener(self._onMessage)
                self._messageListeners[remoteAddress] = listenr
                if (self.listeningToMessages):
                    listenr.start()
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
                print("hkube_api message listener through exception: " +str(e))

    def startMessageListening(self):
        self.listeningToMessages = True
        for listener in self._messageListeners.values():
            gevent.spawn(listener.start)

    def sendMessage(self, msg):
        if (self._messageProducer is None):
            raise Exception('Trying to send a message from a none stream pipeline or after close had been sent to algorithm')
        if (self._messageProducer.nodeNames):
            self._messageProducer.produce(msg)

    def stopStreaming(self):
        if (self.listeningToMessages):
            for listener in self._messageListeners.values():
                listener.close()
        self.listeningToMessages = False
        self._inputListener = []
        if (self._messageProducer is not None):
            self._messageProducer.close()
            self._messageProducer = None

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
                if (typeCheck.isDict(response) and response.get('storageInfo') and self._storage == 'v2'):
                    result = self._dataAdapter.tryGetDataFromPeerOrStorage(
                        response)
                execution.waiter.set(result)
            else:
                execution.waiter.set(None)

        except Exception as e:
            execution.waiter.set(e)
        finally:
            self._executions.pop(execId)

    def start_algorithm(self, algorithmName, input=[], includeResult=True, blocking=False):
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

    def start_raw_subpipeline(self, name, nodes, flowInput, options=None, webhooks=None, includeResult=True,
                              blocking=False):
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
