from __future__ import print_function, division, absolute_import
import os
import sys
import importlib
import logging
from .wc import WebsocketClient
from .hkube_api import HKubeApi

import hkube_python_wrapper.messages as messages
import hkube_python_wrapper.methods as methods
from events import Events
import threading
if (sys.version_info > (3, 0)):
    # Python 3 code in this block
    from queue import Queue
else:
    # Python 2 code in this block
    from Queue import Queue

class Algorunner:
    def __init__(self):
        self._url = None
        self._algorithm = dict()
        self._input = None
        self._events = Events()
        self._loadAlgorithmError = None
        self._connected=False
        self.hkubeApi=None
        self.msg_queue = Queue()

    def loadAlgorithmCallbacks(self, start, init=None, stop=None, exit=None):
        try:
            cwd = os.getcwd()
            logging.info('Initializing algorithm callbacks')
            self._algorithm['start'] = start
            self._algorithm['init'] = init
            self._algorithm['stop'] = stop
            self._algorithm['exit'] = exit
            for m in dir(methods):
                if not m.startswith("__"):
                    method = getattr(methods, m)
                    methodName = method["name"]
                    if self._algorithm[methodName] != None:
                        logging.info('found method {methodName}'.format(methodName=methodName))
                    else:
                        mandatory = "mandatory" if method["mandatory"] else "optional"
                        error = 'unable to find {mandatory} method {methodName}'.format(
                            mandatory=mandatory, methodName=methodName)
                        if (method["mandatory"]):
                            raise Exception(error)
                        logging.error(error)
            # fix start if it has only one argument
            if start.__code__.co_argcount==1:
                self._algorithm['start']=lambda args,api: start(args)
        except Exception as e:
            self._loadAlgorithmError = e
            logging.error(e)

    def loadAlgorithm(self, options):
        try:
            cwd = os.getcwd()
            package = options["path"]
            entry = options["entryPoint"]
            entryPoint = entry.replace("/", ".")
            entryPoint = os.path.splitext(entryPoint)[0]
            __import__(package)
            os.chdir('{cwd}/{package}'.format(cwd=cwd, package=package))
            logging.info('loading {entry}'.format(entry=entry))
            mod = importlib.import_module('.{entryPoint}'.format(
                entryPoint=entryPoint), package=package)
            logging.info('algorithm code loaded')

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
                        logging.info('found method {methodName}'.format(
                            methodName=methodName))
                    except Exception as e:
                        mandatory = "mandatory" if method["mandatory"] else "optional"
                        error = 'unable to find {mandatory} method {methodName}'.format(
                            mandatory=mandatory, methodName=methodName)
                        if (method["mandatory"]):
                            raise Exception(error)
                        logging.error(error)
        except Exception as e:
            self._loadAlgorithmError = e
            logging.error(e)

    def connectToWorker(self, options):
        if (options["url"] is not None):
            self._url = options["url"]
        else:
            self._url = '{protocol}://{host}:{port}'.format(**options)

        self._wsc = WebsocketClient(self.msg_queue)
        self.hkubeApi = HKubeApi(self._wsc, self)
        self._registerToWorkerEvents()

        logging.info('connecting to {url}'.format(url=self._url))
        t = threading.Thread(name="WsThread",target=self._wsc.startWS, args=(self._url, ))
        t.start()
        return t

    def handle(self, command, data):
        logging.info("got %s",command)
        if (command == 'initialize'):
            self._init(data)
        elif (command == 'start'):
            self._start(data)
        if (command == 'algorithmExecutionDone' or command == 'algorithmExecutionError'):
            self.hkubeApi.algorithmExecutionDone(data)
        elif (command == 'subPipelineDone'):
            self.hkubeApi.subPipelineDone(data)
        elif (command == 'subPipelineError'):
            self.hkubeApi.subPipelineError(data)
        elif (command == 'subPipelineStopped'):
            self.hkubeApi.subPipelineStopped(data)

    def get_message(self, blocking=True):
        return self.msg_queue.get(block=blocking)

    def run(self):
        while True:
            (command, data) = self.get_message()
            self.handle(command,data)

    def close(self):
        self._wsc.stopWS()

    def _registerToWorkerEvents(self):
        self._wsc.events.on_connection += self._connection
        self._wsc.events.on_disconnect += self._disconnect
        # self._wsc.events.on_init += self._init
        # self._wsc.events.on_start += self._start
    #     self._wsc.events.on_stop += self._stop
    #     self._wsc.events.on_exit += self._exit

    def _connection(self):
        self._connected=True
        logging.info('connected to ' + self._url)

    def _disconnect(self):
        if self._connected:
            logging.info('disconnected from ' + self._url)
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
            logging.info('Got exit command. Exiting with code', code)
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
            logging.error(error)
            self._wsc.send({
                "command": messages.outgoing["error"],
                "error": {
                    "code": "Failed",
                    "message": str(error)
                }
            })
        except Exception as e:
            logging.error(e)
