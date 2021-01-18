from __future__ import print_function, division, absolute_import


class Outgoing(object):
    # pylint: disable=too-many-instance-attributes
    def __init__(self):
        self.initialized = "initialized"
        self.started = "started"
        self.stopped = "stopped"
        self.stopping = "stopping"
        self.progress = "progress"
        self.error = "errorMessage"
        self.storing = "storing"
        self.dataSourceRequest = "dataSourceRequest"
        self.streamingStatistics = "streamingStatistics"
        self.done = "done"
        self.servingStatus = "servingStatus"
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
        self.dataSourceResponse = "dataSourceResponse"
        self.algorithmExecutionError = "algorithmExecutionError"
        self.algorithmExecutionDone = "algorithmExecutionDone"
        self.subPipelineStarted = "subPipelineStarted"
        self.subPipelineError = "subPipelineError"
        self.subPipelineDone = "subPipelineDone"
        self.subPipelineStopped = "subPipelineStopped"
        self.serviceDiscoveryUpdate = "serviceDiscoveryUpdate"


class Messages(object):
    def __init__(self):
        self.outgoing = Outgoing()
        self.incoming = Incoming()


messages = Messages()
