class Flow:
    def __init__(self, flow, meName):
        self.flow = flow
        self.me = None
        for node in flow:
            if self.me is None:
                if node['source'] == meName:
                    self.me = node

    def isNextInFlow(self, next):
        if (self.me is None):
            return False
        return next in self.me['next']

    def getRestOfFlow(self):
        if (self.me is None):
            return []
        flowcopy = self.flow[:]
        flowcopy.remove(self.me)
        return flowcopy
