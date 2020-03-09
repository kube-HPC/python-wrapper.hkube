import os


def getPath(base, dir, fName):
    return base + os.path.sep + dir + os.path.sep + fName


class FSAdapter:
    def __init__(self, config):
        self.basePath = config['baseDirectory']

    def put(self, options):
        filePath = getPath(self.basePath, options['directoryName'], options['fileName'])
        self.ensure_dir(filePath)
        f = open(filePath, 'w')
        f.write(options['data'])
        f.close()
        pass

    def get(self, options):
        filePath = getPath(self.basePath, options['directoryName'], options['fileName'])
        if not (os.path.exists(filePath)):
            return None
        f = open(filePath, 'r')
        result = f.read()
        f.close()
        return result

    def list(self, options):
        filePath = self.basePath + os.path.sep + options['path']
        if not (os.path.exists(filePath)):
            return None
        recursive_files_in_dir = []
        for r, d, f in os.walk(filePath):
            files_in_dir = []
            relativePath = r.replace(self.basePath, '')
            for fname in f:
                files_in_dir.append(relativePath + os.path.sep + fname)
            recursive_files_in_dir = recursive_files_in_dir + files_in_dir
        return recursive_files_in_dir

    def delete(self, options):
        pass

    def getStream(self, options):
        filePath = getPath(self.basePath, options['directoryName'], options['fileName'])
        if not (os.path.exists(filePath)):
            return None
        f = open(filePath, 'rb')
        return f

    def getOutputStream(self, options):
        filePath = self.basePath + os.path.sep + options['directoryName'] + os.path.sep + options['fileName']
        f = open(filePath, 'wb')
        return f

    def putStream(self, options):
        intStream = options['data']
        outStream = self.getOutputStream(options)
        readBytes = intStream.read(1000)
        while (len(readBytes) > 0):
            outStream.write(readBytes)
            readBytes = intStream.read(1000)
        outStream.close()

    def ensure_dir(self, f):
        d = os.path.dirname(f)
        if not os.path.exists(d):
            os.makedirs(d)
        return os.path.exists(f)
