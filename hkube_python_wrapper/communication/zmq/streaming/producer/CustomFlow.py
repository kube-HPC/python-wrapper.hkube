class CustomFlow:
    def __init__(self, flow, meName):
        self.flow = flow
        self.me = None
        for node in flow:
            if self.me == None:
                if node['source'] == meName:
                    self.me = node

    def isNextInFlow(self, next):
        if (self.me == None):
            return True
        else:
            return next in self.me['next'];

    def getRestOfFlow(self):
        if (self.me == None):
            return []
        else:
            return self.flow.copy().remove(self.me)
