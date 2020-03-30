
class BaseStorageManager(object):
    def __init__(self, adpter, encoding):
        self.adapter = adpter
        self.encoding = encoding

    def put(self, options):
        encoded = self.encoding.encode(options["data"])
        options.update({"data": encoded})
        return self.adapter.put(options)

    def get(self, options):
        data = self.adapter.get(options)
        if(data is None):
            return None
        return self.encoding.decode(data)

    def list(self, options):
        return self.adapter.list(options)

    def listPrefix(self, options):
        return self.adapter.listPrefix(options)

    def delete(self, options):
        self.adapter.delete(options)
