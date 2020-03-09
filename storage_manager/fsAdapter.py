import os


class FSAdapter:
    def __init__(self, config):
        self.basePath = config['baseDirectory']

    def put(self, options):
        filePath = self.basePath + os.path.sep + options['directoryName'] + os.path.sep + options['fileName']
        self.ensure_dir(filePath)
        f = open(filePath, 'w')
        f.write(options['data'])
        f.close()
        pass

    def get(self, options):
        filePath = self.basePath + os.path.sep + options['directoryName'] + os.path.sep + options['fileName']
        f = open(filePath, 'r')
        result = f.read()
        f.close()
        return result

    def list(self, options):
        return None

    def delete(self, options):
        pass

    def getStream(self, options):
        return None

    def putStream(self, options):
        return None

    def ensure_dir(self, f):
        d = os.path.dirname(f)
        if not os.path.exists(d):
            os.makedirs(d)
        return os.path.exists(f)
