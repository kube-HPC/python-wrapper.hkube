from __future__ import print_function, division, absolute_import

from random import randint

from .statelessAlgoWrapper import statelessALgoWrapper
from ..config import config
from .wc import WebsocketClient
from .data_adapter import DataAdapter
from .methods import methods
from .messages import messages
from hkube_python_wrapper.tracing import Tracer
from hkube_python_wrapper.codeApi.hkube_api import HKubeApi
from hkube_python_wrapper.communication.DataServer import DataServer
from events import Events
import gevent
import traceback
import importlib
import sys
import os
from gevent import monkey

monkey.patch_all()


class Algorunner:
    def __init__(self):
        self._url = None
        self._algorithm = dict()
        self._input = None
        self._events = Events()
        self._loadAlgorithmError = None
        self._connected = False
        self._hkubeApi = None
        self._dataAdapter = None
        self._dataServer = None
        self._discovery = None
        self._wsc = None
        self.tracer = None
        self._storage = None
        self._nodeName = None
        self.startCurrentlyRunning = None

    @staticmethod
    def Run(start=None, init=None, stop=None, exit=None, options=None):
        algorunner = Algorunner()
        if (start):
            algorunner.loadAlgorithmCallbacks(
                start, init=init, stop=stop, exit=exit, options=options or config)
        else:
            algorunner.loadAlgorithm(config)
        jobs = algorunner.connectToWorker(config)
        gevent.joinall(jobs)

    @staticmethod
    def Debug(debug_url, start, init=None, stop=None, exit=None, options=None):
        algorunner = Algorunner()
        config.socket['url'] = debug_url
        config.storage['mode'] = 'v1'
        algorunner.loadAlgorithmCallbacks(
            start, init=init, stop=stop, exit=exit, options=options or config)
        jobs = algorunner.connectToWorker(config)
        gevent.joinall(jobs)

    def loadAlgorithmCallbacks(self, start, init=None, stop=None, exit=None, options=None):
        try:
            print('Initializing algorithm callbacks')
            self._algorithm['start'] = start
            self._algorithm['init'] = init
            self._algorithm['stop'] = stop
            self._algorithm['exit'] = exit
            if not (config.discovery.get('streaming').get('stateful')):
                wrapper = statelessALgoWrapper(self._algorithm)
                self._algorithm = dict()
                self._algorithm['start'] = wrapper.start
                self._algorithm['init'] = wrapper.init
                self._algorithm['stop'] = wrapper.stop
                self._algorithm['exit'] = wrapper.exit

            for k, v in methods.items():
                methodName = k
                method = v
                isMandatory = method["mandatory"]
                if self._algorithm[methodName] is not None:
                    print('found method {methodName}'.format(
                        methodName=methodName))
                else:
                    mandatory = "mandatory" if isMandatory else "optional"
                    error = 'unable to find {mandatory} method {methodName}'.format(
                        mandatory=mandatory, methodName=methodName)
                    if (isMandatory):
                        raise Exception(error)
                    print(error)
            # fix start if it has only one argument
            if start.__code__.co_argcount == 1:
                self._algorithm['start'] = lambda args, api: start(args)
            self.tracer = Tracer(options.tracer)

        except Exception as e:
            self._loadAlgorithmError = self._errorMsg(e)
            print(e)

    def loadAlgorithm(self, options):
        try:
            cwd = os.getcwd()
            algOptions = options.algorithm
            package = algOptions["path"]
            entry = algOptions["entryPoint"]
            entryPoint = entry.replace("/", ".")
            entryPoint = os.path.splitext(entryPoint)[0]
            __import__(package)
            os.chdir('{cwd}/{package}'.format(cwd=cwd, package=package))
            print('loading {entry}'.format(entry=entry))
            mod = importlib.import_module('.{entryPoint}'.format(
                entryPoint=entryPoint), package=package)
            print('algorithm code loaded')

            for k, v in methods.items():
                methodName = k
                method = v
                isMandatory = method["mandatory"]
                try:
                    self._algorithm[methodName] = getattr(mod, methodName)
                    # fix start if it has only one argument
                    if methodName == 'start' and self._algorithm['start'].__code__.co_argcount == 1:
                        self._algorithm['startOrig'] = self._algorithm['start']
                        self._algorithm['start'] = lambda args, api: self._algorithm['startOrig'](
                            args)
                    print('found method {methodName}'.format(
                        methodName=methodName))
                except Exception as e:
                    mandatory = "mandatory" if isMandatory else "optional"
                    error = 'unable to find {mandatory} method {methodName}'.format(
                        mandatory=mandatory, methodName=methodName)
                    if (isMandatory):
                        raise Exception(error)
                    print(error)
            self.tracer = Tracer(options.tracer)
        except Exception as e:
            self._loadAlgorithmError = self._errorMsg(e)
            traceback.print_exc()
            print(e)

    def connectToWorker(self, options):
        socket = options.socket
        encoding = socket.get("encoding")
        self._storage = options.storage.get("mode")
        url = socket.get("url")

        if (url is not None):
            self._url = url
        else:
            self._url = '{protocol}://{host}:{port}'.format(**socket)

        self._url += '?storage={storage}&encoding={encoding}'.format(
            storage=self._storage, encoding=encoding)

        self._wsc = WebsocketClient(encoding)
        self._initStorage(options)
        self._hkubeApi = HKubeApi(self._wsc, self._dataAdapter, self._storage)
        self._registerToWorkerEvents()

        print('connecting to {url}'.format(url=self._url))
        job1 = gevent.spawn(self._wsc.startWS, self._url)
        job2 = gevent.spawn(self._dataServer and self._dataServer.listen)
        return [job1, job2]

    def _initStorage(self, options):
        if (self._storage != 'v1'):
            self._initDataServer(options)
        self._initDataAdapter(options)

    def _initDataServer(self, options):
        enable = options.discovery.get("enable")
        if (enable):
            self._discovery = {
                'host': options.discovery.get("host"),
                'port': options.discovery.get("port")
            }
            self._dataServer = DataServer(options.discovery)

    def _initDataAdapter(self, options):
        self._dataAdapter = DataAdapter(options, self._dataServer)

    def close(self):
        self._wsc.shutDown()

    def _registerToWorkerEvents(self):
        self._wsc.events.on_connection += self._connection
        self._wsc.events.on_disconnect += self._disconnect
        self._wsc.events.on_init += self._init
        self._wsc.events.on_start += self._start
        self._wsc.events.on_stop += self._stop
        self._wsc.events.on_exit += self._exit
        self._wsc.events.on_discovery_update += self._discovery_update

    def _connection(self):
        self._connected = True
        print('connected to ' + self._url)

    def _disconnect(self):
        if self._connected:
            print('disconnected from ' + self._url)
        self._connected = False

    def _getMethod(self, name):
        return self._algorithm.get(name)

    def _init(self, options):
        try:
            if (self._loadAlgorithmError):
                self._sendError(self._loadAlgorithmError)
            else:
                self._input = options
                self._nodeName = options.get('nodeName')
                method = self._getMethod('init')
                if (method is not None):
                    method(options)
                self._sendCommand(messages.outgoing.initialized, None)

        except Exception as e:
            self._sendError(e)

    def _discovery_update(self, discovery):
        print('Got discovery update' + str(discovery))
        messageListenrConfig = {'encoding': config.discovery['encoding']}
        self._hkubeApi.setupStreamingListeners(
            messageListenrConfig, discovery, self._nodeName)

    def _start(self, options):
        if (self.isPipeLineStreaming()):
            def onStatistics(statistics):
                self._sendCommand(
                    messages.outgoing.streamingStatistics, statistics)

            producerConfig = {}
            producerConfig["port"] = config.discovery['streaming']['port']
            producerConfig['messagesMemoryBuff'] = config.discovery['streaming']['messagesMemoryBuff']
            producerConfig['encoding'] = config.discovery['encoding']
            producerConfig['statisticsInterval'] = config.discovery['streaming']['statisticsInterval']
            self._hkubeApi.setupStreamingProducer(
                onStatistics, producerConfig, self._input['childs'])
        # pylint: disable=unused-argument
        span = None
        threadId = randint(0, 0x10000)
        try:
            self._sendCommand(messages.outgoing.started, None)
            # TODO: add parent span from worker
            jobId = self._input.get("jobId")
            taskId = self._input.get("taskId")
            nodeName = self._input.get("nodeName")
            info = self._input.get("info", {})
            savePaths = info.get("savePaths", [])
            topSpan = options.get(
                'spanId') if options else self._input.get('spanId')
            span = Tracer.instance.create_span(
                "start", topSpan, jobId, taskId, nodeName)

            newInput = self._dataAdapter.getData(self._input)
            self._input.update({'input': newInput})
            method = self._getMethod('start')
            self.startCurrentlyRunning = threadId
            algorithmData = method(self._input, self._hkubeApi)
            self._handle_response(algorithmData, jobId,
                                  taskId, nodeName, savePaths, span)
            self.startCurrentlyRunning = None
        except Exception as e:
            traceback.print_exc()
            Tracer.instance.finish_span(span, e)
            if(self.startCurrentlyRunning == threadId):
                self._sendError(e)
            else:
                print('Exception from old algorithm run: ' + str(e))

    def _handle_response(self, algorithmData, jobId, taskId, nodeName, savePaths, span):
        if (self._storage == 'v2'):
            self._handle_responseV2(
                algorithmData, jobId, taskId, nodeName, savePaths, span)
        else:
            self._handle_responseV1(algorithmData, span)

    def _handle_responseV1(self, algorithmData, span):
        if (span):
            Tracer.instance.finish_span(span)
        if (self.isPipeLineStreaming()):
            self._hkubeApi.stopStreaming()
        self._sendCommand(messages.outgoing.done, algorithmData)

    def _handle_responseV2(self, algorithmData, jobId, taskId, nodeName, savePaths, span):
        encodedData = self._dataAdapter.encode(algorithmData)

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

        if (self._dataServer and savePaths):
            self._dataServer.setSendingState(taskId, algorithmData)
            storingData.update(
                {'discovery': self._discovery, 'taskId': taskId})
            self._sendCommand(messages.outgoing.storing, storingData)
            self._dataAdapter.setData(
                {'jobId': jobId, 'taskId': taskId, 'data': encodedData})
        else:
            self._dataAdapter.setData(
                {'jobId': jobId, 'taskId': taskId, 'data': encodedData})
            self._sendCommand(messages.outgoing.storing, storingData)
        if (span):
            Tracer.instance.finish_span(span)
        if (self.isPipeLineStreaming()):
            self._hkubeApi.stopStreaming()
        self._sendCommand(messages.outgoing.done, None)

    def _stop(self, options):
        try:
            method = self._getMethod('stop')
            gevent.sleep()
            if (method is not None):
                method(options)
            if (self.isPipeLineStreaming()):
                self._hkubeApi.stopStreaming()
            self._sendCommand(messages.outgoing.stopped, None)
            self.startCurrentlyRunning = None
        except Exception as e:
            self._sendError(e)

    def _exit(self, options):
        try:
            self._dataServer and self._dataServer.shutDown()
            self._wsc.shutDown()
            self._wsc.shutDown()
            method = self._getMethod('exit')
            if (method is not None):
                method(options)

            option = options if options is not None else dict()
            code = option.get('exitCode', 0)
            print('Got exit command. Exiting with code', code)
            sys.exit(code)

        except Exception as e:
            self._sendError(e)

    def _sendCommand(self, command, data):
        try:
            self._wsc.send({'command': command, 'data': data})
        except Exception as e:
            self._sendError(e)

    def _sendError(self, error):
        try:
            print(error)
            self._wsc.send({
                'command': messages.outgoing.error,
                'error': {
                    'code': 'Failed',
                    'message': self._errorMsg(error)
                }
            })
            if (self.isPipeLineStreaming()):
                self._hkubeApi.stopStreaming()
        except Exception as e:
            print(e)

    def _errorMsg(self, error):
        return str(error)

    def isPipeLineStreaming(self):
        if (self._input.get('kind') == 'stream'):
            return True
        else:
            return False

