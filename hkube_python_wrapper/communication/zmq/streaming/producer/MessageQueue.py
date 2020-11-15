from collections import OrderedDict


class MessageQueue(object):
    def __init__(self, consumerTypes):
        self.consumerTypes = consumerTypes

        self.indexPerConsumer = OrderedDict()
        for consumerType in consumerTypes:
            self.indexPerConsumer[consumerType] = 0
        self.sizeSum = 0
        self.sentToAll = 0
        self.lostMessages = 0
        self.queue = []

    def hasItems(self, consumerType):
        return self.indexPerConsumer[consumerType] < len(self.queue)

    # Messages are kept in the queue until consumers of all types popped out the message.
    # An index per consumer type is maintained, to know which messages the consumer already received and conclude which message should he get now.
    def pop(self, consumerType):
        index = self.indexPerConsumer[consumerType]
        out = self.queue[index]
        index += 1
        self.indexPerConsumer[consumerType] = index
        if (index == 1):
            anyZero = False
            for value in self.indexPerConsumer.values():
                if (value == 0):
                    anyZero = True
                    break
            if not (anyZero):
                self.queue.pop(0)
                self.sentToAll += 1
                _, msg = out
                self.sizeSum -= len(msg)
                for key in self.indexPerConsumer.keys():
                    self.indexPerConsumer[key] = self.indexPerConsumer[key] - 1
        return out

    def loseMessage(self):
        out = self.queue.pop(0)
        _, msg = out
        self.sizeSum -= len(msg)
        self.lostMessages += 1

    def append(self, header, msg):
        self.sizeSum += len(msg)
        return self.queue.append((header, msg))

    def size(self, consumerType):
        index = self.indexPerConsumer[consumerType]
        size = len(self.queue) - index
        return size

    def sent(self, consumerType):
        index = self.indexPerConsumer[consumerType]
        sent = self.sentToAll + index
        return sent
