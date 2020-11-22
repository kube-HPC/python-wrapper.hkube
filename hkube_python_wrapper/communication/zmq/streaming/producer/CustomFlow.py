class CustomFlow:
    def __init__(self, flow, meName):
        self.flow = flow
        self.me = None
        for node in flow:
            if self.me is None:
                if node['source'] == meName:
                    self.me = node

    def isNextInFlow(self, next):
        if (self.me is None):
            return True
        return next in self.me['next']

    def getRestOfFlow(self):
        if (self.me is None):
            return []
        return self.flow.copy().remove(self.me)
