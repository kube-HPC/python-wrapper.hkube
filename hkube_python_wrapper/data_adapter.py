from __future__ import print_function, division, absolute_import
import collections
import copy
import six
import util.object_path as objectPath
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
        flatObj = self._flatten(input)

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

                objectPath.setPath(result, k, data)

        return result

    def setData(self, options):
        jobId = options.get("jobId")
        taskId = options.get("taskId")
        data = options.get("data")
        result = self._storageManager.hkube.put(jobId, taskId, data)
        return result

    @classmethod
    def _flatten(cls, inp, sep="/"):

        obj = collections.OrderedDict()

        def recurse(t, parent_key=""):

            if isinstance(t, list):
                for i in range(len(t)):
                    recurse(t[i], parent_key + sep + str(i) if parent_key else str(i))
            elif isinstance(t, dict):
                for k, v in t.items():
                    recurse(v, parent_key + sep + k if parent_key else k)
            else:
                obj[parent_key] = t

        recurse(inp)

        return obj

    def _tryGetDataFromPeerOrStorage(self, options):
        path = options.get("path")
        index = options.get("index")
        discovery = options.get("discovery")
        storageInfo = options.get("storageInfo")

        dataPath = self._createDataPath(path, index)
        data = None

        if (discovery):
            data = self._getFromPeer(options, dataPath)

        if (data is None and storageInfo):
            data = self._getFromStorage(storageInfo)
            data = objectPath.getPath(data, dataPath)

        return data

    @classmethod
    def _createDataPath(cls, path, index):
        dataPath = path
        if isinstance(index, six.integer_types):
            if (path is not None):
                dataPath = '{path}.{index}'.format(path=path, index=index)
            else:
                dataPath = str(index)

        return dataPath.replace(".", "/") if dataPath else None

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
                value = objectPath.getPath(objData, path, separator='.')
                meta = self._getMetadata(value)
                metadata[path] = meta
            except Exception as e:
                pass

        return metadata

    @staticmethod
    def _getMetadata(value):
        if(isinstance(value, dict)):
            meta = {'type': 'object'}
        if(isinstance(value, collections.Sequence)):
            meta = {'type': 'array', 'size': len(value)}
        else:
            meta = {'type': str(type(value).__name__)}
        return meta
