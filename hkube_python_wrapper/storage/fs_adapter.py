import os


class FSAdapter:
    def __init__(self, config):
        self.basePath = config['baseDirectory']

    def init(self, options):
        path = options["path"]
        return self.ensure_dir(path)

    def put(self, options):
        filePath = self.getPath(self.basePath, options['path'])
        self.ensure_dir(filePath)
        with open(filePath, 'wb') as f:
            f.write(options['data'])
        return {'path': options['path']}

    def get(self, options):
        filePath = self.getPath(self.basePath, options['path'])
        if not (os.path.exists(filePath)):
            return None

        result = None
        with open(filePath, 'rb') as f:
            result = f.read()
        return result

    def list(self, options):
        filePath = self.basePath + os.path.sep + options['path']
        if not (os.path.exists(filePath)):
            return None
        recursive_files_in_dir = []
        for r, d, f in os.walk(filePath): # pylint: disable=unused-variable
            files_in_dir = []
            relativePath = r.replace(self.basePath, '')
            for fname in f:
                files_in_dir.append({'path': relativePath + os.path.sep + fname})
            recursive_files_in_dir = recursive_files_in_dir + files_in_dir
        return recursive_files_in_dir

    def delete(self, options):
        filePath = self.getPath(self.basePath, options['path'])
        os.remove(filePath)

    def listPrefix(self, options):
        filePath = self.basePath + os.path.sep + options['path']
        if not (os.path.exists(filePath)):
            return None
        for r, d, f in os.walk(filePath):  # pylint: disable=unused-variable
            files_in_dir = []
            relativePath = r.replace(self.basePath, '')
            for fname in f:
                files_in_dir.append(relativePath + os.path.sep + fname)
            break
        return files_in_dir

    @staticmethod
    def ensure_dir(dirName):
        d = os.path.dirname(dirName)
        if not os.path.exists(d):
            os.makedirs(d)
        return os.path.exists(dirName)

    @staticmethod
    def getPath(base, dirName):
        return base + os.path.sep + dirName
