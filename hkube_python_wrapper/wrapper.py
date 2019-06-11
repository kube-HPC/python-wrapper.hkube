
import os
import sys
import importlib
from .wc import WebsocketClient
import hkube_python_wrapper.messages as messages
import hkube_python_wrapper.methods as methods
from events import Events
import threading
import algorithm_unique_folder

algorithm_path = "algorithm_unique_folder"

class Algorunner:
    def __init__(self, options):
        self._url = None
        self._algorithm = dict()
        self._input = None
        self._events = Events()
        self._loadAlgorithmError = None
        self._bootstrap(options)

    def _bootstrap(self, options):
        self._loadAlgorithm(options)
        self._connectToWorker(options)

    def _loadAlgorithm(self, options):
        try:
            cwd = os.getcwd()
            alg = options.algorithm
            package = algorithm_path
            entry = alg["entryPoint"]
            entryPoint = entry.replace("/", ".")
            entryPoint = os.path.splitext(entryPoint)[0]
            os.chdir('{cwd}/{package}'.format(cwd=cwd, package=package))
            print('loading {entry}'.format(entry=entry))
            mod = importlib.import_module('.{entryPoint}'.format(entryPoint=entryPoint), package=package)
            print('algorithm code loaded')

            for m in dir(methods):
                if not m.startswith("__"):
                    method = getattr(methods, m)
                    methodName = method["name"]
                    try:
                        self._algorithm[methodName] = getattr(mod, methodName)
                        print('found method {methodName}'.format(methodName=methodName))
                    except Exception as e:
                        mandatory = "mandatory" if method["mandatory"] else "optional"
                        error = 'unable to find {mandatory} method {methodName}'.format(mandatory=mandatory, methodName=methodName)
                        if (method["mandatory"]):
                            raise Exception(error)
                        print(error)

        except Exception as e:
            self._loadAlgorithmError = e
            print(e)

    def _connectToWorker(self, options):
        socket = options.socket
        if (socket["url"] is not None):
            self._url = socket["url"]
        else:
            self._url = '{protocol}://{host}:{port}'.format(**socket)

        self._wsc = WebsocketClient()
        self._registerToWorkerEvents()

        print('connecting to {url}'.format(url=self._url))
        t = threading.Thread(target=self._wsc.startWS, args=(self._url, ))
        t.start()

    def _registerToWorkerEvents(self):
        self._wsc.events.on_connection += self._connection
        self._wsc.events.on_disconnect += self._disconnect
        self._wsc.events.on_init += self._init
        self._wsc.events.on_start += self._start
        self._wsc.events.on_stop += self._stop
        self._wsc.events.on_exit += self._exit

    def _connection(self):
        print('connected to ' + self._url)

    def _disconnect(self):
        print('disconnected from '+ self._url)

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

                self._wsc.send({"command": messages.outgoing["initialized"]})

        except Exception as e:
            self._sendError(e)

    def _start(self, options):
        try:
            self._wsc.send({"command": messages.outgoing["started"]})
            method = self._getMethod(methods.start)
            output = method(self._input)
            self._wsc.send({
                "command": messages.outgoing["done"],
                "data": output
            })

        except Exception as e:
            self._sendError(e)

    def _stop(self, options):
        try:
            method = self._getMethod(methods.stop)
            if (method is not None):
                method(options)

            self._wsc.send({"command": messages.outgoing["stopped"]})

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

    def _sendError(self, error):
        print(error)
        self._wsc.send({
            "command": messages.outgoing["error"],
            "error": {
                "code": "Failed",
                "message": str(error)
            }
        })
