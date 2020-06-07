from __future__ import print_function, division, absolute_import


class Outgoing(object):
    def __init__(self):
        self.initialized = "initialized"
        self.started = "started"
        self.stopped = "stopped"
        self.progress = "progress"
        self.error = "errorMessage"
        self.storing = "storing"
        self.done = "done"
        self.startAlgorithmExecution = "startAlgorithmExecution"
        self.stopAlgorithmExecution = "stopAlgorithmExecution"
        self.startRawSubPipeline = "startRawSubPipeline"
        self.startStoredSubPipeline = "startStoredSubPipeline"
        self.stopSubPipeline = "stopSubPipeline"


class Incoming(object):
    def __init__(self):
        self.initialize = "initialize"
        self.start = "start"
        self.stop = "stop"
        self.exit = "exit"
        self.algorithmExecutionError = "algorithmExecutionError"
        self.algorithmExecutionDone = "algorithmExecutionDone"
        self.subPipelineStarted = "subPipelineStarted"
        self.subPipelineError = "subPipelineError"
        self.subPipelineDone = "subPipelineDone"
        self.subPipelineStopped = "subPipelineStopped"


class Messages(object):
    def __init__(self):
        self.outgoing = Outgoing()
        self.incoming = Incoming()


messages = Messages()
