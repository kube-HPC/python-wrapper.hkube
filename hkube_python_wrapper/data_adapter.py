from __future__ import print_function, division, absolute_import
import gevent
import os
import sys
import importlib
from .wc import WebsocketClient
from .hkube_api import HKubeApi
import copy
from events import Events
import collections
import six
import dpath.util


class DataAdapter:

    def getData(self, options):

        input = options["input"]
        storage = options["storage"]

        if (len(storage) == 0):
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

                path = k.replace("_", "/")
                dpath.util.set(result, path, data)

        return result

    def setData(self, options):
        jobId = options.get("jobId")
        taskId = options.get("taskId")
        data = options.get("data")
        result = storageManager.hkube.put(
            {"jobId": jobId, "taskId": taskId, "data": data})
        return result

    def _flatten(self, d, sep="_"):

        obj = collections.OrderedDict()

        def recurse(t, parent_key=""):

            if isinstance(t, list):
                for i in range(len(t)):
                    recurse(t[i], parent_key + sep + str(i)
                            if parent_key else str(i))
            elif isinstance(t, dict):
                for k, v in t.items():
                    recurse(v, parent_key + sep + k if parent_key else k)
            else:
                obj[parent_key] = t

        recurse(d)

        return obj

    def _getPath(self, data, path):
        path = path.replace(".", "/")
        return dpath.util.get(data, path)

    def _tryGetDataFromPeerOrStorage(self, options):
        path = options.get("path")
        index = options.get("index")
        discovery = options.get("discovery")
        storageInfo = options.get("storageInfo")

        dataPath = self._createDataPath(path, index)
        data = None

        if (discovery):
            data = self._getFromPeer(options, dataPath)

        if (data == None and storageInfo):
            data = self._getFromStorage(storageInfo)
            if (dataPath):
                data = self._getPath(data, dataPath)

        return data

    def _createDataPath(self, path, index):
        dataPath = path
        if isinstance(index, six.integer_types):
            if (path is not None):
                dataPath = '{path}.{index}'.format(path=path, index=index)
            else:
                dataPath = str(index)

        return dataPath

    # def _getFromPeer(self, options):
    #     {taskId, dataPath} = options
    #     {port, host, encoding} = options.discovery
    #     dataRequest = new DataRequest({address: {port, host}, taskId, dataPath, encoding})
    #     response = dataRequest.invoke()
    #     self.emit(Events.DiscoveryGet, response)
    #     return response.data

    def _getFromStorage(self, options):
        return options
        data = storageManager.get(options)
        return data

    def createStorageInfo(self, options):
        jobId = options.get("jobId")
        taskId = options.get("taskId")

        path = 'jobId/taskId'
        metadata = self.createMetadata(options)
        storageInfo = {
            "storageInfo": {
                "path": path
            },
            "metadata": metadata
        }
        return storageInfo

    def createMetadata(self, options):
        nodeName = options.get("nodeName")
        data = options.get("data")
        savePaths = options.get("savePaths")

        object = {}
        object[nodeName] = data
        paths = savePaths or []
        metadata = dict()
        for p in paths:
            value = dpath.util.get(object, p, separator='.', default='DEFAULT')
            if (value != 'DEFAULT'):
                self._setMetadata(value, p, metadata)

        return metadata

    def _setMetadata(self, value, path, metadata):
        if(isinstance(value, collections.Sequence)):
            meta = {"type": "array", "size": len(value)}
        else:
            meta = {"type": str(type(value).__name__)}
        metadata[path] = meta
