from __future__ import print_function, division, absolute_import
import os
import sys
import importlib
import traceback
import gevent
from events import Events
from hkube_python_wrapper.communication.DataServer import DataServer
from hkube_python_wrapper.codeApi.hkube_api import HKubeApi
from hkube_python_wrapper.tracing import Tracer
from .messages import messages
from .methods import methods
from .data_adapter import DataAdapter
from .wc import WebsocketClient


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

    def loadAlgorithmCallbacks(self, start, init=None, stop=None, exit=None, options=None):
        try:
            print('Initializing algorithm callbacks')
            self._algorithm['start'] = start
            self._algorithm['init'] = init
            self._algorithm['stop'] = stop
            self._algorithm['exit'] = exit
            for k, v in methods.items():
                methodName = k
                method = v
                isMandatory = method["mandatory"]
                if self._algorithm[methodName] is not None:
                    print('found method {methodName}'.format(methodName=methodName))
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
        storage = options.storage
        encoding = socket.get("encoding")
        storage = storage.get("mode")
        url = socket.get("url")

        if (url is not None):
            self._url = url
        else:
            self._url = '{protocol}://{host}:{port}'.format(**socket)

        self._url += '?storage={storage}&encoding={encoding}'.format(
            storage=storage, encoding=encoding)

        self._wsc = WebsocketClient(encoding)
        self._initStorage(options)
        self._hkubeApi = HKubeApi(self._wsc, self._dataAdapter)
        self._registerToWorkerEvents()

        print('connecting to {url}'.format(url=self._url))
        job1 = gevent.spawn(self._wsc.startWS, self._url)
        job2 = gevent.spawn(self._dataServer and self._dataServer.listen)
        return [job1, job2]

    def _initStorage(self, options):
        self._initDataServer(options)
        self._initDataAdapter(options)

    def _initDataServer(self, options):
        enable = options.discovery.get("enable")
        if(enable):
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
                method = self._getMethod('init')
                if (method is not None):
                    method(options)

                self._sendCommand(messages.outgoing.initialized, None)

        except Exception as e:
            self._sendError(e)

    def _start(self, options):
        # pylint: disable=unused-argument
        span = None
        try:
            self._sendCommand(messages.outgoing.started, None)
            # TODO: add parent span from worker
            jobId = self._input.get("jobId")
            taskId = self._input.get("taskId")
            nodeName = self._input.get("nodeName")
            info = self._input.get("info", {})
            savePaths = info.get("savePaths", [])
            topSpan = self._input.get('spanId')
            span = Tracer.instance.create_span("start", topSpan, jobId, taskId, nodeName)

            newInput = self._dataAdapter.getData(self._input)
            self._input.update({'input': newInput})
            method = self._getMethod('start')
            algorithmData = method(self._input, self._hkubeApi)
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

            if(self._dataServer and savePaths):
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
            Tracer.instance.finish_span(span)
            self._sendCommand(messages.outgoing.done, None)

        except Exception as e:
            traceback.print_exc()
            Tracer.instance.finish_span(span, e)
            self._sendError(e)

    def _stop(self, options):
        try:
            method = self._getMethod('stop')
            if (method is not None):
                method(options)

            self._sendCommand(messages.outgoing.stopped, None)

        except Exception as e:
            self._sendError(e)

    def _exit(self, options):
        try:
            self._dataServer and self._dataServer.shutDown()
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
        except Exception as e:
            print(e)

    def _errorMsg(self, error):
        return str(error)
