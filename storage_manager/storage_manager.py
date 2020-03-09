from .fsAdapter import FSAdapter

class StorageManager:
    def __init__(self,config):
        self.storage = FSAdapter(config)
    def put(self, options):
        return self.storage.put(options)
    def get(self, options):
        return self.storage.get(options)
    def list(self, options):
        return self.storage.list(options)
    def delete(self,options):
        self.storage.delete(options)
    def getStream(self,options):
        return self.storage.getStream(options)
    def putStream(self,options):
        return self.storage.putStream(options)

if __name__ == '__main__':
    config = {'baseDirectory': '/home/golanha/'}
    sm = StorageManager(config)
    options  = {}
    options['fileName'] = 'a.txt'
    options['directoryName'] = 'c'
    options['data'] = 'ssfff'
    sm.put(options)

