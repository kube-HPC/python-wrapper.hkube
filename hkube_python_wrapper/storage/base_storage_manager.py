
class BaseStorageManager(object):
    def __init__(self, adpter):
        self.adapter = adpter

    def put(self, options):
        return self.adapter.put(options)

    def get(self, options):
        data = self.adapter.get(options)
        if(data is None):
            return None
        return data

    def list(self, options):
        return self.adapter.list(options)

    def listPrefix(self, options):
        return self.adapter.listPrefix(options)

    def delete(self, options):
        self.adapter.delete(options)
