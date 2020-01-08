from __future__ import print_function, division, absolute_import
import gevent
import os
import sys
import importlib
from .wc import WebsocketClient
from .hkube_api import HKubeApi

import hkube_python_wrapper.messages as messages
import hkube_python_wrapper.methods as methods
from events import Events


class Algorunner:
    def __init__(self):
        self._url = None
        self._algorithm = dict()
        self._input = None
        self._events = Events()
        self._loadAlgorithmError = None
        self._connected=False
        self.hkubeApi=None

    def loadAlgorithmCallbacks(self, start, init=None, stop=None, exit=None):
        try:
            cwd = os.getcwd()
            print('Initializing algorithm callbacks')
            self._algorithm['start'] = start
            self._algorithm['init'] = init
            self._algorithm['stop'] = stop
            self._algorithm['exit'] = exit
            for m in dir(methods):
                if not m.startswith("__"):
                    method = getattr(methods, m)
                    methodName = method["name"]
                    if self._algorithm[methodName] != None:
                        print('found method {methodName}'.format(methodName=methodName))
                    else:
                        mandatory = "mandatory" if method["mandatory"] else "optional"
                        error = 'unable to find {mandatory} method {methodName}'.format(
                            mandatory=mandatory, methodName=methodName)
                        if (method["mandatory"]):
                            raise Exception(error)
                        print(error)
            # fix start if it has only one argument
            if start.__code__.co_argcount==1:
                self._algorithm['start']=lambda args,api: start(args)
        except Exception as e:
            self._loadAlgorithmError = e
            print(e)

    def loadAlgorithm(self, options):
        try:
            cwd = os.getcwd()
            package = options["path"]
            entry = options["entryPoint"]
            entryPoint = entry.replace("/", ".")
            entryPoint = os.path.splitext(entryPoint)[0]
            __import__(package)
            os.chdir('{cwd}/{package}'.format(cwd=cwd, package=package))
            print('loading {entry}'.format(entry=entry))
            mod = importlib.import_module('.{entryPoint}'.format(
                entryPoint=entryPoint), package=package)
            print('algorithm code loaded')

            for m in dir(methods):
                if not m.startswith("__"):
                    method = getattr(methods, m)
                    methodName = method["name"]
                    try:
                        self._algorithm[methodName] = getattr(mod, methodName)
                        # fix start if it has only one argument
                        if methodName=='start' and self._algorithm['start'].__code__.co_argcount==1:
                            self._algorithm['startOrig']=self._algorithm['start']
                            self._algorithm['start']=lambda args,api: self._algorithm['startOrig'](args)
                        print('found method {methodName}'.format(
                            methodName=methodName))
                    except Exception as e:
                        mandatory = "mandatory" if method["mandatory"] else "optional"
                        error = 'unable to find {mandatory} method {methodName}'.format(
                            mandatory=mandatory, methodName=methodName)
                        if (method["mandatory"]):
                            raise Exception(error)
                        print(error)
        except Exception as e:
            self._loadAlgorithmError = e
            print(e)

    def connectToWorker(self, options):
        if (options["url"] is not None):
            self._url = options["url"]
        else:
            self._url = '{protocol}://{host}:{port}'.format(**options)
        binary = options.get("binary", "False") in ['True', 'true']

        self._wsc = WebsocketClient(binary=binary)
        self.hkubeApi = HKubeApi(self._wsc)
        self._registerToWorkerEvents()

        print('connecting to {url}'.format(url=self._url))
        job = gevent.spawn(self._wsc.startWS,self._url)
        return job

    def close(self):
        self._wsc.stopWS()

    def _registerToWorkerEvents(self):
        self._wsc.events.on_connection += self._connection
        self._wsc.events.on_disconnect += self._disconnect
        self._wsc.events.on_init += self._init
        self._wsc.events.on_start += self._start
        self._wsc.events.on_stop += self._stop
        self._wsc.events.on_exit += self._exit

    def _connection(self):
        self._connected=True
        print('connected to ' + self._url)

    def _disconnect(self):
        if self._connected:
            print('disconnected from ' + self._url)
        self._connected=False

    def _getMethod(self, method):
        return self._algorithm.get(method["name"])

    def _init(self, options):
        try:
            if (self._loadAlgorithmError):
                self._sendError(self._loadAlgorithmError)
            else:
                self._input = options
                method = self._getMethod(methods.init)
                if (method is not None):
                    method(options)

                self._sendCommand(messages.outgoing["initialized"], None)

        except Exception as e:
            self._sendError(e)

    def _start(self, options):
        try:
            self._sendCommand(messages.outgoing["started"], None)
            method = self._getMethod(methods.start)
            output = method(self._input, self.hkubeApi)
            self._sendCommand(messages.outgoing["done"], output)

        except Exception as e:
            self._sendError(e)

    def _stop(self, options):
        try:
            method = self._getMethod(methods.stop)
            if (method is not None):
                method(options)

            self._sendCommand(messages.outgoing["stopped"], None)

        except Exception as e:
            self._sendError(e)

    def _exit(self, options):
        try:
            self._wsc.stopWS()
            method = self._getMethod(methods.exit)
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
            self._wsc.send({"command": command, "data": data})
        except Exception as e:
            self._sendError(e)

    def _sendError(self, error):
        try:
            print(error)
            self._wsc.send({
                "command": messages.outgoing["error"],
                "error": {
                    "code": "Failed",
                    "message": str(error)
                }
            })
        except Exception as e:
            print(e)
