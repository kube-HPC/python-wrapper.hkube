import os
import sys
import errno
from hkube_python_wrapper.util.encoding import Encoding

PY3 = sys.version_info[0] == 3


class FSAdapter:
    def __init__(self, config, encoding):
        self.basePath = config['baseDirectory']
        self.encoding = Encoding(encoding)

    def init(self, options):
        path = options["path"]
        return self.ensure_dir(path)

    def put(self, options):
        filePath = self.getPath(self.basePath, options['path'])
        self.ensure_dir(filePath)
        data = options['data']
        header = options.get('header')

        with open(filePath, 'wb') as f:
            if(header):
                f.write(header)
            f.write(data)
        return {'path': options['path']}

    def get(self, options):
        filePath = self.getPath(self.basePath, options['path'])
        header = None
        payload = None
        headerLength = self.encoding.headerLength()
        with open(filePath, 'rb') as f:
            header = f.read(headerLength)
            if(not self.encoding.isHeader(header)):
                f.seek(0)
            payload = f.read()
        return (header, payload)

    def list(self, options):
        filePath = self.basePath + os.path.sep + options['path']
        if not (os.path.exists(filePath)):
            return None
        recursive_files_in_dir = []
        for r, d, f in os.walk(filePath):  # pylint: disable=unused-variable
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
        return FSAdapter.ensure_dir_py3(dirName) if PY3 else FSAdapter.ensure_dir_py27(dirName)

    @staticmethod
    def ensure_dir_py3(dirName):
        d = os.path.dirname(dirName)
        # pylint: disable=unexpected-keyword-arg
        os.makedirs(d, exist_ok=True)
        return os.path.exists(dirName)

    @staticmethod
    def ensure_dir_py27(dirName):
        d = os.path.dirname(dirName)
        try:
            os.makedirs(d)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        return os.path.exists(dirName)

    @staticmethod
    def getPath(base, dirName):
        return base + os.path.sep + dirName
