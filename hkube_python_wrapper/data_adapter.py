from __future__ import print_function, division, absolute_import
import json
import util.type_check as typeCheck
from util.decorators import timing
from util.object_path import getPath, setPath
from util.encoding import Encoding
from storage.storage_manager import StorageManager
from communication.DataRequest import DataRequest


class DataAdapter:
    def __init__(self, options, dataServer=None):
        self._dataServer = dataServer
        self._storageCache = dict()
        self._encoding = Encoding(options['encoding'])
        self._storageManager = StorageManager(options)

    def encode(self, value):
        return self._encoding.encode(value)

    def decode(self, value):
        return self._encoding.decode(value)

    def getData(self, options):
        inputArgs = options.get("input")
        flatInput = options.get("flatInput")
        storage = options.get("storage")
        useCache = options.get("useCache")

        if (flatInput is None or len(flatInput) == 0):
            return inputArgs

        if (useCache is False):
            self._storageCache = dict()
            print('using clean cache')
        else:
            print('using old cache')

        for k, v in flatInput.items():
            if self._isStorage(v):
                key = v[2:]
                link = storage.get(key, None)
                if (link is None):
                    raise Exception('unable to find storage key')

                data = None
                if(typeCheck.isList(link)):
                    data = list(map(self._tryGetDataFromPeerOrStorage, link))
                else:
                    data = self._tryGetDataFromPeerOrStorage(link)

                setPath(inputArgs, k, data)

        return inputArgs

    def _isStorage(self, value):
        return typeCheck.isString(value) and value.startswith('$$')

    def setData(self, options):
        jobId = options.get("jobId")
        taskId = options.get("taskId")
        data = options.get("data")
        result = self._storageManager.hkube.put(jobId, taskId, data)
        return result

    def _tryGetDataFromPeerOrStorage(self, options):
        path = options.get("path")
        discovery = options.get("discovery")
        storageInfo = options.get("storageInfo")
        data = None
        hasResponse = False

        if (discovery):
            data = self._getFromPeer(options, path)
            hasResponse = self._hasPeerResponse(data)

        if (not hasResponse and storageInfo):
            data = self._getFromCacheOrStorage(storageInfo)
            if(path):
                data = getPath(data, path)

        return data

    @timing
    def _getFromPeer(self, options, dataPath):
        taskId = options.get('taskId')
        discovery = options.get('discovery')
        port = discovery.get('port')
        host = discovery.get('host')
        encoding = discovery.get('encoding')

        response = None
        if(self._dataServer and host == self._dataServer._host and port == self._dataServer._port):
            res = self._dataServer.createData({'taskId': taskId, 'dataPath': dataPath})
            response = self._encoding.decode(res)

        else:
            request = {
                'address': {
                    'port': port,
                    'host': host
                },
                'taskId': taskId,
                'dataPath': dataPath,
                'encoding': encoding
            }
            dataRequest = DataRequest(request)
            response = dataRequest.invoke()

        return response

    def _hasPeerResponse(self, options):
        if(typeCheck.isDict(options)):
            error = options.get('hkube_error')
            if(error is not None):
                print(json.dumps(error, indent=2))
                return False

        return True

    def _getFromCacheOrStorage(self, options):
        path = options.get('path')
        data = self._getFromCache(path)
        if (data is None):
            data = self._getFromStorage(options)
            self._setToCache(path, data)

        return data

    def _getFromCache(self, path):
        return self._storageCache.get(path)

    def _setToCache(self, path, data):
        self._storageCache[path] = data

    @timing
    def _getFromStorage(self, options):
        response = self._storageManager.storage.get(options)
        return self._encoding.decode(response)

    def createStorageInfo(self, options):
        jobId = options.get("jobId")
        taskId = options.get("taskId")
        encodedData = options.get("encodedData")

        path = self._storageManager.hkube.createPath(jobId, taskId)
        metadata = self.createMetadata(options)

        storageInfo = {
            'storageInfo': {
                'path': path,
                'size':  len(encodedData)
            },
            'metadata': metadata
        }
        return storageInfo

    def createMetadata(self, options):
        nodeName = options.get("nodeName")
        data = options.get("data")
        savePaths = options.get("savePaths", [])

        metadata = dict()
        objData = dict()
        objData[nodeName] = data
        for path in savePaths:
            try:
                value = getPath(objData, path)
                if (value != 'DEFAULT'):
                    meta = self._getMetadata(value)
                    metadata[path] = meta
            except Exception as e:
                pass

        return metadata

    @staticmethod
    def _getMetadata(value):
        if(typeCheck.isDict(value)):
            meta = {'type': 'object'}
        elif(typeCheck.isList(value)):
            meta = {'type': 'array', 'size': len(value)}
        else:
            meta = {'type': str(type(value).__name__)}
        return meta
