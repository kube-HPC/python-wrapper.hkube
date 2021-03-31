from collections import OrderedDict
from hkube_python_wrapper.communication.zmq.streaming.producer.Flow import Flow
import threading


class MessageQueue(object):
    def __init__(self, consumerTypes, nodeName):
        self.nodeName = nodeName
        self.consumerTypes = consumerTypes
        self.lock = threading.Lock()
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
            messageFlowPattern, _, _, _ = self.queue[index]
            flow = Flow(messageFlowPattern)
            if (flow.isNextInFlow(consumerType, self.nodeName)):
                foundMessage = True
            else:
                index += 1

        if (foundMessage):
            return index
        return None

    # Messages are kept in the queue until consumers of all types popped out the message.
    # An index per consumer type is maintained, to know which messages the consumer already received and conclude which message should he get now.
    def pop(self, consumerType):
        with self.lock:
            nextItemIndex = self.nextMessageIndex(consumerType)
            if (nextItemIndex is not None):
                out = self.queue[nextItemIndex]
                index = nextItemIndex + 1
                self.indexPerConsumer[consumerType] = index
                self.sent[consumerType] += 1
                while(self.removeIfNeeded()):
                    pass
                return out
            return None

    def removeIfNeeded(self):
        if (self.queue):
            anyZero = False
            out = self.queue[0]
            for consumerType, index in self.indexPerConsumer.items():
                if (index == 0):
                    messageFlowPattern, _, _, _ = out
                    flow = Flow(messageFlowPattern)
                    if (flow.isNextInFlow(consumerType, self.nodeName)):
                        anyZero = True
                        break

            if not (anyZero):
                self.queue.pop(0)
                _, _, msg, _ = out
                self.sizeSum -= len(msg)
                for consumerType in self.indexPerConsumer.keys():
                    if (self.indexPerConsumer[consumerType] > 0):
                        self.indexPerConsumer[consumerType] = self.indexPerConsumer[consumerType] - 1
                return True
        return False

    def loseMessage(self):
        with self.lock:
            if (self.queue):
                out = self.queue.pop(0)
                messageFlowPattern, _, msg, _ = out
                self.sizeSum -= len(msg)
                for consumerType in self.indexPerConsumer.keys():
                    if self.indexPerConsumer[consumerType] > 0:
                        self.indexPerConsumer[consumerType] = self.indexPerConsumer[consumerType] - 1
                    else:
                        flow = Flow(messageFlowPattern)
                        if (flow.isNextInFlow(consumerType, self.nodeName)):
                            self.lostMessages[consumerType] += 1

    # TODO: TRY TO IMPROVE THIS
    def append(self, messageFlowPattern, header, msg, appendTime):
        with self.lock:
            flow = Flow(messageFlowPattern)
            hasRecipient = False
            for consumerType in self.consumerTypes:
                if (flow.isNextInFlow(consumerType, self.nodeName)):
                    self.everAppended[consumerType] += 1
                    hasRecipient = True
            if (hasRecipient):
                self.sizeSum += len(msg)
                self.queue.append((messageFlowPattern, header, msg, appendTime))

    def size(self, consumerType):
        everAppended = self.everAppended[consumerType]
        size = everAppended - self.sent[consumerType] - self.lostMessages[consumerType]
        return size
