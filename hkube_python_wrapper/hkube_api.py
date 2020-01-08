from __future__ import print_function, division, absolute_import
from .execution import AlgorithmExecution
from .waitFor import WaitForData
import hkube_python_wrapper.messages as messages
import threading
import logging
from events import Events
import sys
import os
import time
if (sys.version_info > (3, 0)):
    # Python 3 code in this block
    from queue import Queue, Empty
else:
    # Python 2 code in this block
    from Queue import Queue, Empty


class HKubeApi:
    def __init__(self, wc, wrapper):
        self._wc = wc
        self._wrapper=wrapper
        self._algorithmExecutionsMap = {}
        self._lastExecId = 0
        # self._wc.events.on_algorithmExecutionDone += self.algorithmExecutionDone
        # self._wc.events.on_algorithmExecutionError += self.algorithmExecutionDone

        # self._wc.events.on_subPipelineDone += self.subPipelineDone
        # self._wc.events.on_subPipelineError += self.subPipelineDone
        # self._wc.events.on_subPipelineStopped += self.subPipelineDone



    def algorithmExecutionDone(self, data):
        logging.debug('got done with execId %s',data.get('execId'))
        execution = self._algorithmExecutionsMap.get(data.get('execId'))
        execution.waiter.set(data)

    def subPipelineDone(self, data):
        execution = self._algorithmExecutionsMap.get(data.get('subPipelineId'))
        execution.waiter.set(data)

    def start_algorithm(self, algorithmName, input=[], resultAsRaw=False, blocking=False):
        logging.info('start_algorithm called with {name}'.format(name=algorithmName))
        self._lastExecId += 1
        execId = str(self._lastExecId)
        execution = AlgorithmExecution(execId, WaitForData(True))
        self._algorithmExecutionsMap[execId] = execution

        message = {
            "command": messages.outgoing["startAlgorithmExecution"],
            "data": {
                "execId": execId,
                "algorithmName": algorithmName,
                "input": input,
                "resultAsRaw": resultAsRaw
            }
        }
        self._wc.send(message)

        while not execution.waiter.ready():
            try:
                (command, data) = self._wrapper.get_message(False)
                self._wrapper.handle(command, data)
            except Empty:
                pass
            time.sleep(0.01)
        
        logging.debug('result execId %s',execId)

        return execution.waiter.get()
        
        # if blocking:
        #     return execution.waiter.get()
        # return execution.waiter

    def start_stored_subpipeline(self, name, flowInput={}, blocking=False):
        logging.info('start_stored_subpipeline called with {name}'.format(name=name))
        self._lastExecId += 1
        execId = str(self._lastExecId)
        execution = AlgorithmExecution(execId, WaitForData(True))
        self._algorithmExecutionsMap[execId] = execution

        message = {
            "command": messages.outgoing["startStoredSubPipeline"],
            "data": {
                "subPipeline": {
                    "name": name,
                    "flowInput": flowInput
                },
                "subPipelineId": execId,

            }
        }
        self._wc.send(message)

        while not execution.waiter.ready():
            (command, data) = self._wrapper.get_message()
            self._wrapper.handle(command, data)
        return execution.waiter.get()
        # if blocking:
        #     return execution.waiter.get()
        # return execution.waiter

    def start_raw_subpipeline(self, name, nodes, flowInput, options=None, webhooks=None, blocking=False):
        logging.info('start_raw_subpipeline called with {name}'.format(name=name))
        self._lastExecId += 1
        execId = str(self._lastExecId)
        execution = AlgorithmExecution(execId, WaitForData(True))
        self._algorithmExecutionsMap[execId] = execution

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
                "subPipelineId": execId,

            }
        }
        self._wc.send(message)

        while not execution.waiter.ready():
            (command, data) = self._wrapper.get_message()
            self._wrapper.handle(command, data)
        return execution.waiter.get()
        # if blocking:
        #     return execution.waiter.get()
        # return execution.waiter
