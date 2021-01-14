from collections import OrderedDict

from hkube_python_wrapper.communication.zmq.streaming.producer.Flow import Flow


class MessageQueue(object):
    def __init__(self, consumerTypes, me):
        self.me = me
        self.consumerTypes = consumerTypes

        self.indexPerConsumer = OrderedDict()
        self.sent = {}
        self.everAppended = {}
        self.lostMessages = {}
        for consumerType in self.consumerTypes:
            self.indexPerConsumer[consumerType] = 0
            self.sent[consumerType] = 0
            self.everAppended[consumerType] = 0
            self.lostMessages[consumerType] = 0
        self.sizeSum = 0
        self.queue = []

    def hasItems(self, consumerType):
        return self.indexPerConsumer[consumerType] < len(self.queue)

    def nextMessageIndex(self, consumerType):
        index = self.indexPerConsumer[consumerType]
        foundMessage = False
        while (not foundMessage) and index < len(self.queue):
            messageFlowPattern, _, _ = self.queue[index]
            flow = Flow(messageFlowPattern)
            if (flow.isNextInFlow(consumerType, self.me)):
                foundMessage = True
            else:
                index += 1

        if (foundMessage):
            return index
        return None

    # Messages are kept in the queue until consumers of all types popped out the message.
    # An index per consumer type is maintained, to know which messages the consumer already received and conclude which message should he get now.
    def pop(self, consumerType):
        nextItemIndex = self.nextMessageIndex(consumerType)
        if (nextItemIndex is not None):
            out = self.queue[nextItemIndex]
            index = nextItemIndex + 1
            self.indexPerConsumer[consumerType] = index
            self.sent[consumerType] += 1
            anyZero = False
            for value in self.indexPerConsumer.values():
                if (value == 0):
                    anyZero = True
                    break

            if not (anyZero):
                self.queue.pop(0)
                _, _, msg = out
                self.sizeSum -= len(msg)
                for key in self.indexPerConsumer.keys():
                    self.indexPerConsumer[key] = self.indexPerConsumer[key] - 1
            return out
        return None

    def loseMessage(self):
        out = self.queue.pop(0)
        _, _, msg = out
        self.sizeSum -= len(msg)
        for key in self.indexPerConsumer.keys():
            if self.indexPerConsumer[key] > 0:
                self.indexPerConsumer[key] = self.indexPerConsumer[key] - 1
            else:
                self.lostMessages[key] += 1

    def append(self, messageFlowPattern, header, msg):
        self.sizeSum += len(msg)
        flow = Flow(messageFlowPattern)
        for consumerType in self.consumerTypes:
            if (flow.isNextInFlow(consumerType, self.me)):
                self.everAppended[consumerType] += 1
        return self.queue.append((messageFlowPattern, header, msg))

    def size(self, consumerType):
        everAppended = self.everAppended[consumerType]
        size = everAppended - self.sent[consumerType]
        return size
