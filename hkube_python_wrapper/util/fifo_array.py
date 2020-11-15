class FifoArray(object):
    def __init__(self, size):
        self.arr = []
        self.size = size

    def append(self, obj):
        self.arr.append(obj)
        if (len(self.arr) > self.size):
            self.arr.pop(0)

    def getAsArray(self):
        return self.arr

    def reset(self):
        self.arr = []
