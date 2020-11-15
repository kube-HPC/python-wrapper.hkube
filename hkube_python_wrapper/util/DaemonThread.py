from threading import Thread


class DaemonThread(Thread):
    def __init__(self, name=''):
        Thread.__init__(self, name=name)
        self.daemon = True
