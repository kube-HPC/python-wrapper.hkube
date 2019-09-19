from __future__ import print_function, division, absolute_import

outgoing = {
    "initialized": "initialized",
    "started": "started",
    "stopped": "stopped",
    "progress": "progress",
    "error": "errorMessage",
    "done": "done",
    "startAlgorithmExecution": "startAlgorithmExecution",
    "stopAlgorithmExecution": "stopAlgorithmExecution",
    "startRawSubPipeline": "startRawSubPipeline",
    "startStoredSubPipeline": "startStoredSubPipeline",
    "stopSubPipeline": "stopSubPipeline"

}
incoming = {
    "initialize": "initialize",
    "start": "start",
    "stop": "stop",
    "algorithmExecutionError": "algorithmExecutionError",
    "algorithmExecutionDone": "algorithmExecutionDone",
    "subPipelineStarted": "subPipelineStarted",
    "subPipelineError": "subPipelineError",
    "subPipelineDone": "subPipelineDone",
    "subPipelineStopped": "subPipelineStopped",
}
