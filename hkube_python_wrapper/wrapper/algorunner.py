from __future__ import print_function, division, absolute_import

import time

from hkube_python_wrapper.util.DaemonThread import DaemonThread
from .statelessAlgoWrapper import statelessAlgoWrapper
from ..config import config
from .wc import WebsocketClient
from .data_adapter import DataAdapter
from .job import Job
from .methods import methods
from .messages import messages
from hkube_python_wrapper.tracing import Tracer
from hkube_python_wrapper.codeApi.hkube_api import HKubeApi
from hkube_python_wrapper.communication.DataServer import DataServer
from hkube_python_wrapper.communication.streaming.StreamingManager import StreamingManager
from hkube_python_wrapper.util.queueImpl import Queue, Empty
from hkube_python_wrapper.util.timerImpl import Timer
from hkube_python_wrapper.util.logger import log
from hkube_python_wrapper.util.url_encode_impl import url_encode
from hkube_python_wrapper.util.stdout_redirector import stdout_redirector
import os
import sys
import platform
import importlib
import traceback
from threading import Thread, current_thread


class Algorunner(DaemonThread):
    def __init__(self):
        self._url = None
        self._originalAlgorithm = dict()
        self._statelessWrapped = dict()
        self._algorithm = None
        self._input = None
        self._job = None
        self._loadAlgorithmError = None
        self._connected = False
        self._hkubeApi = None
        self.streamingManager = None
        self._msg_queue = Queue()
        self._dataAdapter = None
        self._dataServer = None
        self._discovery = None
        self._wsc = None
        self._tracer = None
        self._storage = None
        self._active = True
        self._runningStartThread = None
        self._stopped = False
        self._redirectLogs = False
        DaemonThread.__init__(self, "WorkerListener")

    @staticmethod
    def Run(start=None, init=None, stop=None, exit=None, options=None):
        """Starts the algorunner wrapper.

        Convenience method to start the algorithm. Pass the algorithm methods
        This method blocks forever

        Args:
            start (function): The entry point of the algorithm. Called for every invocation.
            init (function): Optional init method. Called for every invocation before the start.
            stop (function): Optional stop method. Called when the parent pipeline is stopped.
            exit (function): Optional exit handler. Called before the algorithm is forced to exit.
                    Can be used to clean up resources.

        Returns:
            Never returns.
        """
        algorunner = Algorunner()
        if (start):
            algorunner.loadAlgorithmCallbacks(
                start, init=init, stop=stop, exit=exit, options=options or config)
        else:
            algorunner.loadAlgorithm(config)
        jobs = algorunner.connectToWorker(config)
        for j in jobs:
            j.join()

    @staticmethod
    def Debug(debug_url, start, init=None, stop=None, exit=None, options=None, logs=False):
        """Starts the algorunner wrapper and registers for local run.

        Convenience method to start the algorithm. Pass the algorithm methods
        This method blocks forever

        Args:
            debug_url (string) The websocket url of the debug algorithm in the kubernetes cluster.
            start (function): The entry point of the algorithm. Called for every invocation.
            init (function): Optional init method. Called for every invocation before the start.
            stop (function): Optional stop method. Called when the parent pipeline is stopped.
            exit (function): Optional exit handler. Called before the algorithm is forced to exit.
                    Can be used to clean up resources.
            logs (bool): Optional. If True will send logs to the cluster. Default: False

        Returns:
            Never returns.
        """
        algorunner = Algorunner()
        options = options or config
        options.socket['url'] = debug_url
        options.storage['mode'] = 'v1'
        options.discovery["enable"] = False
        options.algorithm['redirectLogs'] = logs
        algorunner.loadAlgorithmCallbacks(start, init=init, stop=stop, exit=exit, options=options)
        jobs = algorunner.connectToWorker(config)
        for j in jobs:
            j.join()

    def loadAlgorithmCallbacks(self, start, init=None, stop=None, exit=None, options=None):
        try:
            log.info('Initializing algorithm callbacks')
            self._originalAlgorithm['start'] = start
            self._originalAlgorithm['init'] = init
            self._originalAlgorithm['stop'] = stop
            self._originalAlgorithm['exit'] = exit
            for k, v in methods.items():
                methodName = k
                method = v
                isMandatory = method["mandatory"]
                if self._originalAlgorithm[methodName] is not None:
                    log.info('found method {methodName}', methodName=methodName)
                else:
                    mandatory = "mandatory" if isMandatory else "optional"
                    error = 'unable to find {mandatory} method {methodName}'.format(
                        mandatory=mandatory, methodName=methodName)
                    if (isMandatory):
                        raise Exception(error)
                    log.warning(error)
            # fix start if it has only one argument
            if start.__code__.co_argcount == 1:
                self._originalAlgorithm['start'] = lambda args, api: start(args)
            self._wrapStateless()
            self._tracer = Tracer(options.tracer)

        except Exception as e:
            self._loadAlgorithmError = self._errorMsg(e)
            log.error(e)

    @staticmethod
    def _getEntryPoint(entry):
        splits = os.path.splitext(entry)
        entryPoint = splits[0] if splits[-1] == '.py' else entry
        entryPoint = entryPoint.replace("/", ".")
        return entryPoint

    def loadAlgorithm(self, options):
        try:
            cwd = os.getcwd()
            algOptions = options.algorithm
            package = algOptions["path"]
            entry = algOptions["entryPoint"]
            entryPoint = Algorunner._getEntryPoint(entry)
            __import__(package)
            os.chdir('{cwd}/{package}'.format(cwd=cwd, package=package))
            log.info('loading {entry}', entry=entry)
            mod = importlib.import_module('.{entryPoint}'.format(
                entryPoint=entryPoint), package=package)
            log.info('algorithm code loaded')

            for k, v in methods.items():
                methodName = k
                method = v
                isMandatory = method["mandatory"]
                try:
                    self._originalAlgorithm[methodName] = getattr(mod, methodName)
                    # fix start if it has only one argument
                    if methodName == 'start' and self._originalAlgorithm['start'].__code__.co_argcount == 1:
                        self._originalAlgorithm['startOrig'] = self._originalAlgorithm['start']
                        self._originalAlgorithm['start'] = lambda args, api: self._originalAlgorithm['startOrig'](
                            args)
                    log.info('found method {methodName}', methodName=methodName)
                except Exception:
                    mandatory = "mandatory" if isMandatory else "optional"
                    error = 'unable to find {mandatory} method {methodName}'.format(
                        mandatory=mandatory, methodName=methodName)
                    if (isMandatory):
                        raise Exception(error)
                    log.warning(error)
            self._wrapStateless()
            self._tracer = Tracer(options.tracer)
        except Exception as e:
            self._loadAlgorithmError = self._errorMsg(e)
            traceback.print_exc()
            log.error(e)

    def _wrapStateless(self):
        wrapper = statelessAlgoWrapper(self._originalAlgorithm)
        self._statelessWrapped['start'] = wrapper.start
        self._statelessWrapped['init'] = wrapper.init
        self._statelessWrapped['stop'] = wrapper.stop
        self._statelessWrapped['exit'] = wrapper.exit

    def connectToWorker(self, options):
        socket = options.socket
        encoding = socket.get("encoding")
        self._storage = options.storage.get("mode")
        self._redirectLogs = options.algorithm.get("redirectLogs")
        url = socket.get("url")

        if (url is not None):
            self._url = url
        else:
            self._url = '{protocol}://{host}:{port}'.format(**socket)

        self._url += '?storage={storage}&encoding={encoding}'.format(
            storage=self._storage, encoding=encoding)
        if (self._redirectLogs):
            self._url += '&hostname={hostname}'.format(hostname=url_encode(platform.node()))
        self._wsc = WebsocketClient(self._msg_queue, encoding, self._url)
        self._initDataAdapter(options)
        self.streamingManager = StreamingManager()
        self._hkubeApi = HKubeApi(self._wsc, self, self._dataAdapter, self._storage, self.streamingManager)
        self._registerToWorkerEvents()

        log.info('connecting to {url}', url=self._url)
        self._wsc.start()
        self.start()
        return [self._wsc, self]

    def handle(self, command, data):
        if command == messages.incoming.initialize:
            self._init(data)
        elif command == messages.incoming.start:
            self._start(data)
        elif command == messages.incoming.stop:
            self._stopAlgorithm(data)
        elif command == messages.incoming.streamingInMessage:
            self.streamingManager.onMessage(messageFlowPattern=None, msg=data['payload'], origin=data['origin'], sendMessageId=data['sendMessageId'])
            self._wsc.send({'command': messages.outgoing.streamingInMessageDone, 'data': {'sendMessageId': data['sendMessageId']}})
        elif command == messages.incoming.serviceDiscoveryUpdate:
            self._discovery_update(data)
        elif command == messages.incoming.exit:
            # call exit on different thread to prevent deadlock
            Timer(0.1, lambda: self._exit(data), name="Exit timer").start()
        elif command in [messages.incoming.algorithmExecutionDone, messages.incoming.algorithmExecutionError]:
            self._hkubeApi.algorithmExecutionDone(data)
        elif command in [messages.incoming.subPipelineDone, messages.incoming.subPipelineError, messages.incoming.subPipelineStopped]:
            self._hkubeApi.subPipelineDone(data)
        elif command == messages.incoming.dataSourceResponse:
            self._hkubeApi.dataSourceResponse(data)
        elif command == messages.incoming.alreadyConnectedError:
            log.error('Debugger already connected from {hostname}'.format(hostname=data['hostname']))
            # pylint: disable=protected-access
            os._exit(1)

    def get_message(self, blocking=True):
        return self._msg_queue.get(block=blocking, timeout=0.1)

    def run(self):
        while self._active:
            try:
                (command, data) = self.get_message()
                runThread = Thread(name=command + "Thread", target=self.handle, args=[command, data])
                runThread.daemon = True
                runThread.start()
            except Empty:
                pass
        log.info('Exiting run loop')

    def close(self):
        if (self._wsc):
            self._wsc.shutDown()
        self._active = False

    def getCurrentJob(self):
        return self._job

    def _initDataServer(self, options):
        enable = options.discovery.get("enable")
        if (enable and self._storage != 'v1' and self._job is not None):
            if (self._job.isStreaming and self._dataServer is not None):
                self._dataServer.shutDown()
                self._dataServer = None

            elif (not self._job.isStreaming and self._dataServer is None):
                self._discovery = {
                    'host': options.discovery.get("host"),
                    'port': options.discovery.get("port")
                }
                self._dataServer = DataServer(options.discovery)
                self._dataServer.listen()
                self._reportServing(interval=options.discovery.get("servingReportInterval"))

    def _initDataAdapter(self, options):
        self._dataAdapter = DataAdapter(options, self._dataServer)

    def _registerToWorkerEvents(self):
        self._wsc.events.on_connection += self._connection
        self._wsc.events.on_disconnect += self._disconnect

    def _connection(self):
        self._connected = True
        log.info('connected to {url}', url=self._url)

    def _disconnect(self):
        if self._connected:
            log.info('disconnected from {url}', url=self._url)
        self._connected = False

    def _log_message(self, data):
        try:
            self._sendCommand(messages.outgoing.logData, data)
        except Exception:
            pass

    def _getMethod(self, name):
        return self._algorithm.get(name)

    def _init(self, options):
        redirector = None
        try:
            if (self._redirectLogs):
                redirector = stdout_redirector()
                redirector.events.on_data += self._log_message
            if (self._loadAlgorithmError):
                self.sendError(self._loadAlgorithmError)
            else:
                self._input = options
                self._job = Job(options)
                if self._job.isStreaming and not self._job.isStateful:
                    self._algorithm = self._statelessWrapped
                else:
                    self._algorithm = self._originalAlgorithm
                method = self._getMethod('init')
                if (method is not None):
                    method(options)
                self._sendCommand(messages.outgoing.initialized, None)

        except Exception as e:
            self.sendError(e)
        finally:
            if (redirector):
                redirector.flush()
                redirector.events.on_data -= self._log_message
                redirector.cleanup()

    def _discovery_update(self, discovery):
        log.debug('Got discovery update {discovery}', discovery=discovery)
        messageListenerConfig = {'encoding': config.discovery['encoding']}
        self.streamingManager.setupStreamingListeners(
            messageListenerConfig, discovery, self._job.nodeName)

    def _setupStreamingProducer(self, nodeName):
        def onStatistics(statistics):
            self._sendCommand(messages.outgoing.streamingStatistics, statistics)

        producerConfig = {}
        producerConfig["port"] = config.discovery['streaming']['port']
        producerConfig['messagesMemoryBuff'] = config.discovery['streaming']['messagesMemoryBuff']
        producerConfig['encoding'] = config.discovery['encoding']
        producerConfig['statisticsInterval'] = config.discovery['streaming']['statisticsInterval']
        self.streamingManager.setupStreamingProducer(
            onStatistics, producerConfig, self._job.childs, nodeName)

    def _start(self, options):
        if (self._job.isStreaming):
            self.streamingManager.setParsedFlows(self._job.parsedFlow, self._job.defaultFlow)
            if (self._job.childs):
                self._setupStreamingProducer(self._job.nodeName)
                self.streamingManager.clearMessageListeners()
        # pylint: disable=unused-argument
        span = None
        self._initDataServer(config)
        self._runningStartThread = current_thread()
        self._stopped = False
        redirector = None
        try:
            self._sendCommand(messages.outgoing.started, None)
            if (self._redirectLogs):
                redirector = stdout_redirector()
                redirector.events.on_data += self._log_message
            # TODO: add parent span from worker
            jobId = self._job.jobId
            taskId = self._job.taskId
            nodeName = self._job.nodeName
            info = self._job.info or {}
            savePaths = info.get("savePaths", [])
            if (options):
                topSpan = options.get('spanId')
            else:
                topSpan = self._job.spanId
            span = Tracer.instance.create_span("start", topSpan, jobId, taskId, nodeName)
            newInput = self._dataAdapter.getData(self._input)
            self._input.update({'input': newInput})
            method = self._getMethod('start')
            algorithmData = method(self._input, self._hkubeApi)
            if not (self._stopped):
                self._handle_response(algorithmData, jobId, taskId, nodeName, savePaths, span)

        except Exception as e:
            traceback.print_exc()
            Tracer.instance.finish_span(span, e)
            self.sendError(e)
        finally:
            if (redirector):
                redirector.flush()
                redirector.events.on_data -= self._log_message
                redirector.cleanup()
            self._runningStartThread = None

    def _handle_response(self, algorithmData, jobId, taskId, nodeName, savePaths, span):
        if (self._storage == 'v3'):
            self._handle_responseV2_V3(algorithmData, jobId, taskId, nodeName, savePaths, span)
        else:
            self._handle_responseV1(algorithmData, span)

    def _handle_responseV1(self, algorithmData, span):
        if (span):
            Tracer.instance.finish_span(span)
        self._sendCommand(messages.outgoing.done, algorithmData)

    def _handle_responseV2_V3(self, algorithmData, jobId, taskId, nodeName, savePaths, span):
        header, encodedData = self._dataAdapter.encode(algorithmData)

        data = {
            'jobId': jobId,
            'taskId': taskId,
            'nodeName': nodeName,
            'data': algorithmData,
            'encodedData': encodedData,
            'savePaths': savePaths
        }
        storingData = dict()
        storageInfo = self._dataAdapter.createStorageInfo(data)
        storingData.update(storageInfo)
        incache = None
        if (self._dataServer and savePaths):
            incache = self._dataServer.setSendingState(taskId, header, encodedData, len(encodedData))
        if (incache):
            storingData.update({'discovery': self._discovery, 'taskId': taskId})
            self._sendCommand(messages.outgoing.storing, storingData)
            self._dataAdapter.setData({'jobId': jobId, 'taskId': taskId, 'header': header, 'data': encodedData})
        else:
            self._dataAdapter.setData({'jobId': jobId, 'taskId': taskId, 'header': header, 'data': encodedData})
            self._sendCommand(messages.outgoing.storing, storingData)
        if (span):
            Tracer.instance.finish_span(span)
        if (self._job.isStreaming):
            self._hkubeApi.stopStreaming(force=False)
        self._sendCommand(messages.outgoing.done, None)

    def _reportServing(self, interval=None):
        if (interval is None):
            return
        interval = interval / 1000

        def reportInterval():
            if (not self._active or not self._dataServer):
                return
            self._reportServingStatus()
            Timer(interval, reportInterval, name='reportIntervalTimer').start()

        reportInterval()

    def _reportServingStatus(self):
        if (self._dataServer):
            isServing = self._dataServer.isServing()
            if (isServing):
                self._sendCommand(messages.outgoing.servingStatus, True)

    def _stopAlgorithm(self, options):
        if (self._stopped):
            log.warning('Got stop command while already stopping')
        else:
            self._stopped = True
            try:
                method = self._getMethod('stop')
                if (method is not None):
                    method(options)

                forceStop = options.get('forceStop', False)
                if (forceStop is True):
                    log.info('stopping using force flag')
                else:
                    log.info('stopping gracefully')

                if (self._job.isStreaming):
                    if (forceStop is False):
                        stoppingState = True

                        def stopping():
                            while (stoppingState):
                                self._sendCommand(messages.outgoing.stopping, None)
                                time.sleep(1)

                        stoppingThread = Thread(target=stopping)
                        stoppingThread.start()
                        self._hkubeApi.stopStreaming(force=forceStop)
                        stoppingState = False
                        stoppingThread.join()
                        log.info('Joined threads send stopping and stop streaming')
                        self._checkQueueSize(event='scale down')
                    else:
                        self._hkubeApi.stopStreaming(force=forceStop)

                if (self._runningStartThread):
                    self._runningStartThread.join()
                    log.info('Joined threads algorithm and stop algorithm')

                self._sendCommand(messages.outgoing.stopped, None)

            except Exception as e:
                self.sendError(e)

    def _exit(self, options):
        try:
            self._dataServer and self._dataServer.shutDown()
            self._wsc.shutDown()
            method = self._getMethod('exit')
            if (method is not None):
                method(options)
            self._checkQueueSize(event='exit')
            option = options if options is not None else dict()
            code = option.get('exitCode', 0)
            self._active = False
            log.info('Got exit command. Exiting with code {code}', code=code)
            sys.exit(code)

        except Exception as e:
            log.error('Got error during exit: {e}', e=e)
            # pylint: disable=protected-access
            os._exit(0)

    def _checkQueueSize(self, event):
        if (self._job.isStreaming):
            if (self.streamingManager.messageProducer):
                try:
                    log.info('Messages left in queue on {event}={queue}', event=event, queue=str(len(self.streamingManager.messageProducer.adapter.messageQueue.queue)))
                except Exception:
                    log.error('Failed to print number of messages left in queue on {event}', event=event)
            else:
                log.info('MessageProducer already None on {event}', event=event)

    def _sendCommand(self, command, data):
        try:
            self._wsc.send({'command': command, 'data': data})
        except Exception as e:
            self.sendError(e)

    def sendError(self, error):
        try:
            log.error(error)
            self._wsc.send({
                'command': messages.outgoing.error,
                'error': {
                    'code': 'Failed',
                    'message': self._errorMsg(error)
                }
            })
            if (self._job.isStreaming):
                self._hkubeApi.stopStreaming(False)
        except Exception as e:
            log.error(e)

    def _errorMsg(self, error):
        return str(error)
