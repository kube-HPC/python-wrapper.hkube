class Flow:
    def __init__(self, flow,currentName):
        self.flow = flow
        self._current = self._getCurrent(currentName)

    def isNextInFlow(self, next):
        if (self._current is None):
            return False
        return next in self._current['next']

    def getRestOfFlow(self):
        if (self._current is None):
            return []
        flowcopy = self.flow[:]
        flowcopy.remove(self._current)
        return flowcopy

    def _getCurrent(self, currentName):
        for node in self.flow:
            if node['source'] == currentName:
                current = node
                return current
        return None
