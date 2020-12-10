class Flow:
    def __init__(self, flow):
        self.flow = flow

    def isNextInFlow(self, next, currentName):
        current = self._getCurrent(currentName)
        if (current is None):
            return False
        return next in current['next']

    def getRestOfFlow(self, currentName):
        current = self._getCurrent(currentName)
        if (current is None):
            return []
        flowcopy = self.flow[:]
        flowcopy.remove(current)
        return flowcopy

    def _getCurrent(self, currentName):
        current = None
        for node in self.flow:
            if current is None:
                if node['source'] == currentName:
                    current = node
                    return current
        return None
