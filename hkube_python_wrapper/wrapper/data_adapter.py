from __future__ import print_function, division, absolute_import
import multiprocessing
import concurrent.futures
import hkube_python_wrapper.util.type_check as typeCheck
from hkube_python_wrapper.util.decorators import timing, trace
from hkube_python_wrapper.util.object_path import getPath, setPath
from hkube_python_wrapper.util.encoding import Encoding
from hkube_python_wrapper.storage.storage_manager import StorageManager
from hkube_python_wrapper.communication.DataRequest import DataRequest
from hkube_python_wrapper.cache.caching import Cache
from ..config import config


class DataAdapter:
    def __init__(self, options, dataServer=None):
        self._dataServer = dataServer
        self._storageCache = Cache(config.storage)
        self._encoding = Encoding(options.storage['encoding'])
        self._storageManager = StorageManager(options.storage)
        self._requestEncoding = options.storage['encoding']
        self._requestTimeout = options.discovery['timeout']
        self._networkTimeout = options.discovery['networkTimeout']
        self._maxWorkers = min(32, (multiprocessing.cpu_count() or 1) + 4)
        print('using {workers} workers for DataAdapter'.format(workers=self._maxWorkers))

    def encode(self, value):
        return self._encoding.encode(value)

    def decode(self, header=None, value=None):
        return self._encoding.decode(header=header, value=value)

    @trace()
    def getData(self, options):
        jobId = options.get('jobId')
        inputArgs = options.get('input')
        flatInput = options.get('flatInput')
        storage = options.get('storage')

        if (not flatInput):
            return inputArgs

        for k, v in flatInput.items():
            if self._isStorage(v):
                key = v[2:]
                link = storage.get(key, None)
                if (link is None):
                    raise Exception('unable to find storage key')

                if (typeCheck.isList(link)):
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
        header = options.get('header')
        data = options.get('data')
        result = self._storageManager.hkube.put(jobId, taskId, header=header, value=data)
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
            storageResult = self._getFromCacheOrStorage(storageInfo, dataPath, storageInfo.get("path"))
            batchResponse.append(storageResult)
            return batchResponse
        tasksNotInCache, batchResponse = self._storageCache.getAll(tasks)
        if (tasksNotInCache):
            options['tasks'] = tasksNotInCache
            results = self._getFromPeer(options)
            for i, item in enumerate(results):
                size, content = item
                peerError = self._getPeerError(content)
                taskId = tasksNotInCache[i]
                if (peerError):
                    storageData = self._getDataForTask(jobId, taskId, dataPath)
                    batchResponse.append(storageData)
                else:
                    self._storageCache.update(taskId, content, size)
                    content = self._getPath(content, dataPath)
                    batchResponse.append(content)

        return batchResponse

    def _getDataForTask(self, jobId, taskId, dataPath):
        path = self._storageManager.hkube.createPath(jobId, taskId)
        return self._getFromCacheOrStorage({'path': path}, dataPath, taskId)

    def tryGetDataFromPeerOrStorage(self, options):
        dataPath = options.get('path')
        storageInfo = options.get('storageInfo')
        discovery = options.get('discovery')
        data = None
        hasResponse = False
        if (discovery):
            cacheId = options.get('taskId')
        else:
            cacheId = storageInfo.get('path')
        data = self._getFromCache(cacheId, dataPath)
        if not (data):
            if (discovery):
                size, data = self._getFromPeer(options)[0]
                peerError = self._getPeerError(data)
                hasResponse = not peerError
                data = None if peerError else data
                if (hasResponse):
                    self._setToCache(cacheId, data, size)
                    data = self._getPath(data, dataPath)
            if (not hasResponse and storageInfo):
                data = self._getFromCacheOrStorage(storageInfo, dataPath, cacheId)

        return data

    @trace(name='getFromPeer')
    @timing
    def _getFromPeer(self, options):
        taskId = options.get('taskId')
        tasks = [taskId] if taskId else options.get('tasks')
        discovery = options.get('discovery')
        port = discovery.get('port')
        host = discovery.get('host')

        if (self._dataServer and self._dataServer.isLocal(host, port)):
            dataList = self._dataServer.getDataByTaskId(tasks)
            responses = []
            for header, payload in dataList:
                responses.append((len(payload), self.decode(header=header, value=payload)))
        else:
            request = {
                'address': {
                    'port': port,
                    'host': host
                },
                'tasks': tasks,
                'encoding': self._requestEncoding,
                'timeout': self._requestTimeout,
                'networkTimeout': self._networkTimeout
            }
            dataRequest = DataRequest(request)
            responses = dataRequest.invoke()
        return responses

    def _getPeerError(self, options):
        error = None
        if (typeCheck.isDict(options)):
            error = options.get('hkube_error')

        return error

    def _getFromCacheOrStorage(self, options, dataPath, cacheID):
        data = self._getFromCache(cacheID, dataPath)
        if (data is None):
            size, data = self._getFromStorage(options)
            self._setToCache(cacheID, data, size)
            data = self._getPath(data, dataPath)

        return data

    @trace(name='getFromCache')
    @timing
    def _getFromCache(self, cacheId, dataPath):
        data = self._storageCache.get(cacheId)
        data = self._getPath(data, dataPath)
        return data

    def _setToCache(self, cacheId, data, size):
        self._storageCache.update(cacheId, data, size)

    @trace(name='getFromStorage')
    @timing
    def _getFromStorage(self, options):
        (header, payload) = self._storageManager.storage.get(options)
        decoded = self.decode(header=header, value=payload)
        size = len(payload)
        return (size, decoded)

    def createStorageInfo(self, options):
        jobId = options.get('jobId')
        taskId = options.get('taskId')
        encodedData = options.get('encodedData')

        path = self._storageManager.hkube.createPath(jobId, taskId)
        metadata = self.createMetadata(options)

        storageInfo = {
            'storageInfo': {
                'path': path,
                'size': len(encodedData) if encodedData else 0
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
        if (typeCheck.isDict(value)):
            meta = {'type': 'object'}
        elif (typeCheck.isList(value)):
            meta = {'type': 'array', 'size': len(value)}
        else:
            meta = {'type': str(type(value).__name__)}
        return meta

    def _getPath(self, data, dataPath):
        if (data and dataPath):
            newData = getPath(data, dataPath)
            if (newData == 'DEFAULT'):
                newData = None
        else:
            newData = data
        return newData
