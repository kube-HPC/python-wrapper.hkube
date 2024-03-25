import multiprocessing
import time

from hkube_python_wrapper.communication.streaming.MessageProducer import MessageProducer

from hkube_python_wrapper.util.logger import log
from hkube_python_wrapper.util.url_encode_impl import url_encode
from hkube_python_wrapper.wrapper.wc import WebsocketClient
import platform
from hkube_python_wrapper.util.queueImpl import Queue
from hkube_python_wrapper.wrapper.messages import messages



class ProducerRunner:
    def __init__(self, queue, options, childs, producerConfig, node):
        self._options = options
        self.node = node
        self._processQueue = queue
        self._wsc = None
        self.messageProducer = None
        self._childs = childs
        self._producerConfig = producerConfig
        self._msg_queue = None

    def run(self):
        self._msg_queue = Queue()
        self.connectToWorker(self._options)
        self._setupStreamingProducer(self.node)
        while True:
            arg = self._processQueue.get()  # Get data from the queue
            if arg.get("action") == 'stop':
                print ("got stop")
                self.messageProducer.close(arg.get("force"))
                print("stopeped")
                self._processQueue.put("done");
                print("sent back")
                break  # Exit loop if 'exit' is received
            if (arg.get("action")=="queuesize"):
                self._processQueue.put(len(self.messageProducer.adapter.messageQueue.queue))
            self.messageProducer.produce(arg.get("flow"), arg.get("msg"))

    def _setupStreamingProducer(self, nodeName):
        def onStatistics(statistics):
            self._sendCommand(messages.outgoing.streamingStatistics, statistics)

        self.messageProducer = MessageProducer(self._producerConfig, self._childs, self.node)
        self.messageProducer.registerStatisticsListener(onStatistics)
        if (self._childs):
            self.messageProducer.start()

    def connectToWorker(self, options):
        socket = options.socket
        encoding = socket.get("encoding")
        self._storage = options.storage.get("mode")
        self._redirectLogs = options.algorithm.get("redirectLogs")
        url = socket.get("url")

        if (url is not None):
            self._url = url
        else:
            self._url = '{protocol}://{host}:{port}'.format(**socket)

        self._url += '?storage={storage}&encoding={encoding}'.format(
            storage=self._storage, encoding=encoding)
        if (self._redirectLogs):
            self._url += '&hostname={hostname}'.format(hostname=url_encode(platform.node()))
        self._wsc = WebsocketClient(self._msg_queue, encoding, self._url)
        log.info('producer connecting to {url}', url=self._url)
        self._wsc.start()
        return [self._wsc, self]

    def _sendCommand(self, command, data):
        try:
            self._wsc.send({'command': command, 'data': data})
        except Exception as e:
            self.sendError(e)

    def sendError(self, error):
        try:
            log.error(error)
            self._wsc.send({
                'command': messages.outgoing.error,
                'error': {
                    'code': 'Failed',
                    'message': str(error)
                }
            })
        except Exception as e:
            log.error(e)


def method_in_new_process(queue, options, job, node):
    producerRunner = ProducerRunner(queue, options, job, node)
    producerRunner.run()


def start_new_process(options, node):
    queue = multiprocessing.Queue()  # Create a queue

    p = multiprocessing.Process(target=method_in_new_process, args=(queue, options, node,))
    p.start()
    return queue, p


if __name__ == "__main__":
    queue, process = start_new_process()

    # Send data to the new process
    queue.put("Hello from the original process!")
    time.sleep(2)  # Simulate some delay

    # Send more data
    queue.put("Another message!")

    # Terminate the process
    queue.put("exit")
    process.join()

    print("Original process continuing...")
