class BaseStorageManager(object):
    def __init__(self, adpter):
        self.adapter = adpter

    def put(self, options):
        return self.adapter.put(options)

    def get(self, options):
        return self.adapter.get(options)

    def list(self, options):
        return self.adapter.list(options)

    def listPrefix(self, options):
        return self.adapter.listPrefix(options)

    def delete(self, options):
        self.adapter.delete(options)
