
class BaseStorageManager(object):
    def __init__(self, adpter):
        self.adapter = adpter

    def put(self, options):
        try:
            return self.adapter.put(options)
        except Exception:
            raise Exception('Failed to write data to storage')

    def get(self, options):
        try:
            data = self.adapter.get(options)
            if(data is None):
                return None
            return data
        except Exception:
            raise Exception('Failed to read data from storage')


    def list(self, options):
        try:
            return self.adapter.list(options)
        except Exception:
            raise Exception('Failed to list storage data')

    def listPrefix(self, options):
        try:
            return self.adapter.listPrefix(options)
        except Exception:
            raise Exception('Failed to listPrefix storage data')

    def delete(self, options):
        try:
            self.adapter.delete(options)
        except Exception:
            raise Exception('Failed to delete storage data')
