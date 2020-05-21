from __future__ import print_function, division, absolute_import
import multiprocessing
import concurrent.futures
import hkube_python_wrapper.util.type_check as typeCheck
from hkube_python_wrapper.util.decorators import timing, trace
from hkube_python_wrapper.util.object_path import getPath, setPath
from hkube_python_wrapper.util.encoding import Encoding
from hkube_python_wrapper.storage.storage_manager import StorageManager
from hkube_python_wrapper.communication.DataRequest import DataRequest


class DataAdapter:
    def __init__(self, options, dataServer=None):
        self._dataServer = dataServer
        self._storageCache = dict()
        self._encoding = Encoding(options.storage['encoding'])
        self._storageManager = StorageManager(options.storage)
        self._requestEncoding = options.discovery['encoding']
        self._requestTimeout = options.discovery['timeout']
        self._maxWorkers = min(32, (multiprocessing.cpu_count() or 1) + 4)

    def encode(self, value):
        return self._encoding.encode(value)

    def decode(self, value):
        return self._encoding.decode(value)

    @trace()
    def getData(self, options):
        jobId = options.get('jobId')
        inputArgs = options.get('input')
        flatInput = options.get('flatInput')
        storage = options.get('storage')
        useCache = options.get('useCache')

        if (not flatInput):
            return inputArgs

        if (useCache is False):
            self._storageCache = dict()

        for k, v in flatInput.items():
            if self._isStorage(v):
                key = v[2:]
                link = storage.get(key, None)
                if (link is None):
                    raise Exception('unable to find storage key')

                if(typeCheck.isList(link)):
                    data = self.batchRequest(link, jobId)
                else:
                    data = self.tryGetDataFromPeerOrStorage(link)

                setPath(inputArgs, k, data)

        return inputArgs

    def _isStorage(self, value):
        return typeCheck.isString(value) and value.startswith('$$')

    @trace()
    def setData(self, options):
        jobId = options.get('jobId')
        taskId = options.get('taskId')
        data = options.get('data')
        result = self._storageManager.hkube.put(jobId, taskId, data)
        return result

    @timing
    def batchRequest(self, options, jobId):
        batchResponse = []
        for d in options:
            d.update({"jobId": jobId})

        with concurrent.futures.ThreadPoolExecutor(max_workers=self._maxWorkers) as executor:
            for out in executor.map(self._batchRequest, options):
                batchResponse += out

        return batchResponse

    def _batchRequest(self, options):
        batchResponse = []
        jobId = options.get('jobId')
        tasks = options.get('tasks')
        dataPath = options.get('path')
        storageInfo = options.get('storageInfo')
        if (storageInfo):
            storageResult = self._getFromCacheOrStorage(storageInfo, dataPath)
            batchResponse.append(storageResult)
            return batchResponse

        peerResponse = self._getFromPeer(options, dataPath)
        peerError = self._getPeerError(peerResponse)

        if(peerError):
            message = peerError.get('message')
            print('batch request has failed with {message}, using storage fallback'.format(message=message))
            for t in tasks:
                storageData = self._getDataForTask(jobId, t, dataPath)
                batchResponse.append(storageData)
        else:
            errors = peerResponse.get('errors')
            items = peerResponse.get('items')

            if(errors):
                for i, t in enumerate(items):
                    peerError = self._getPeerError(t)
                    if(peerError):
                        taskId = tasks[i]
                        storageData = self._getDataForTask(jobId, taskId, dataPath)
                        batchResponse.append(storageData)
                    else:
                        batchResponse.append(t)
            else:
                batchResponse += items

        return batchResponse

    def _getDataForTask(self, jobId, taskId, dataPath):
        path = self._storageManager.hkube.createPath(jobId, taskId)
        return self._getFromCacheOrStorage({'path': path}, dataPath)

    def tryGetDataFromPeerOrStorage(self, options):
        dataPath = options.get('path')
        storageInfo = options.get('storageInfo')
        discovery = options.get('discovery')
        data = None
        hasResponse = False

        if (discovery):
            data = self._getFromPeer(options, dataPath)
            peerError = self._getPeerError(data)
            hasResponse = not peerError
            data = None if peerError else data

        if (not hasResponse and storageInfo):
            data = self._getFromCacheOrStorage(storageInfo, dataPath)

        return data

    @timing
    def _getFromPeer(self, options, dataPath):
        tasks = options.get('tasks')
        taskId = options.get('taskId')
        discovery = options.get('discovery')
        port = discovery.get('port')
        host = discovery.get('host')

        response = None
        if(self._dataServer and self._dataServer.isLocal(host, port)):
            response = self._dataServer.createData(taskId, tasks, dataPath)

        else:
            request = {
                'address': {
                    'port': port,
                    'host': host
                },
                'tasks': tasks,
                'taskId': taskId,
                'dataPath': dataPath,
                'encoding': self._requestEncoding,
                'timeout': self._requestTimeout
            }
            dataRequest = DataRequest(request)
            response = dataRequest.invoke()

        return response

    def _getPeerError(self, options):
        error = None
        if(typeCheck.isDict(options)):
            error = options.get('hkube_error')

        return error

    def _getFromCacheOrStorage(self, options, dataPath):
        path = options.get('path')
        data = self._getFromCache(path)
        if (data is None):
            data = self._getFromStorage(options)
            self._setToCache(path, data)

        if(dataPath):
            data = getPath(data, dataPath)

        return data

    @timing
    def _getFromCache(self, path):
        return self._storageCache.get(path)

    def _setToCache(self, path, data):
        self._storageCache[path] = data

    @timing
    def _getFromStorage(self, options):
        response = self._storageManager.storage.get(options)
        return self._encoding.decode(response)

    def createStorageInfo(self, options):
        jobId = options.get('jobId')
        taskId = options.get('taskId')
        encodedData = options.get('encodedData')

        path = self._storageManager.hkube.createPath(jobId, taskId)
        metadata = self.createMetadata(options)

        storageInfo = {
            'storageInfo': {
                'path': path,
                'size':  len(encodedData) if encodedData else 0
            },
            'metadata': metadata
        }
        return storageInfo

    def createMetadata(self, options):
        nodeName = options.get('nodeName')
        data = options.get('data')
        savePaths = options.get('savePaths', [])

        metadata = dict()
        objData = dict()
        objData[nodeName] = data
        for path in savePaths:
            try:
                value = getPath(objData, path)
                if (value != 'DEFAULT'):
                    meta = self._getMetadata(value)
                    metadata[path] = meta
            except Exception:
                pass

        return metadata

    def _getMetadata(self, value):
        if(typeCheck.isDict(value)):
            meta = {'type': 'object'}
        elif(typeCheck.isList(value)):
            meta = {'type': 'array', 'size': len(value)}
        else:
            meta = {'type': str(type(value).__name__)}
        return meta
