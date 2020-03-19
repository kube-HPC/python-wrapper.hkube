from __future__ import print_function, division, absolute_import
import collections
import copy
import six
from util.object_path import flatten, getPath, setPath, createDataPath
from storage.storage_manager import StorageManager
from communication.DataRequest import DataRequest


class DataAdapter:
    def __init__(self, options):
        self._storageManager = StorageManager(options)

    def getData(self, options):

        input = options.get("input")
        storage = options.get("storage")

        if (input is None or len(input) == 0):
            return input

        if (storage is None or len(storage) == 0):
            return input

        result = copy.deepcopy(input)
        flatObj = flatten(input)

        for k, v in flatObj.items():
            if isinstance(v, six.string_types) and v.startswith('$$'):
                key = v[2:]
                link = storage.get(key, None)
                if (link is None):
                    raise Exception('unable to find storage key')

                data = None
                if(isinstance(link, collections.Sequence)):
                    data = list(map(self._tryGetDataFromPeerOrStorage, link))
                else:
                    data = self._tryGetDataFromPeerOrStorage(link)

                setPath(result, k, data)

        return result

    def setData(self, options):
        jobId = options.get("jobId")
        taskId = options.get("taskId")
        data = options.get("data")
        result = self._storageManager.hkube.put(jobId, taskId, data)
        return result

    def _tryGetDataFromPeerOrStorage(self, options):
        path = options.get("path")
        index = options.get("index")
        discovery = options.get("discovery")
        storageInfo = options.get("storageInfo")

        dataPath = createDataPath(path, index)
        data = None

        if (discovery):
            data = self._getFromPeer(options, dataPath)

        if (data is None and storageInfo):
            data = self._getFromStorage(storageInfo)
            data = getPath(data, dataPath)

        return data

    def _getFromPeer(self, options, dataPath):
        taskId = options.get('taskId')
        discovery = options.get('discovery')
        port = discovery.get('port')
        host = discovery.get('host')
        encoding = discovery.get('encoding')

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
        # self.emit(Events.DiscoveryGet, response)
        return response.get('data')

    def _getFromStorage(self, options):
        data = self._storageManager.storage.get(options)
        return data

    def createStorageInfo(self, options):
        jobId = options.get("jobId")
        taskId = options.get("taskId")

        path = self._storageManager.hkube.createPath(jobId, taskId)
        metadata = self.createMetadata(options)
        storageInfo = {
            'storageInfo': {
                'path': path
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
        if(isinstance(value, dict)):
            meta = {'type': 'object'}
        elif(isinstance(value, collections.Sequence)):
            meta = {'type': 'array', 'size': len(value)}
        else:
            meta = {'type': str(type(value).__name__)}
        return meta
