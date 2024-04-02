import multiprocessing
from hkube_python_wrapper.communication.streaming.MessageProducer import MessageProducer
from hkube_python_wrapper.util.logger import log
from hkube_python_wrapper.wrapper.messages import messages



class ProducerRunner:
    def __init__(self, method_invoke_queue, statistics_queue, options, childs, producerConfig, node):
        self._options = options
        self.node = node
        self._method_invoke_queue = method_invoke_queue
        self._statistics_queue = statistics_queue
        self._wsc = None
        self.messageProducer = None
        self._childs = childs
        self._producerConfig = producerConfig

    def run(self):
        self._setupStreamingProducer()
        while True:
            arg = self._method_invoke_queue.get()  # Get data from the queue
            if arg.get("action") == 'stop':
                print ("got stop")
                self.messageProducer.close(arg.get("force"))
                print("stopeped")
                self._method_invoke_queue.put("done");
                print("sent back")
                break  # Exit loop if 'exit' is received
            if (arg.get("action")=="queuesize"):
                self._method_invoke_queue.put(len(self.messageProducer.adapter.messageQueue.queue))
            self.messageProducer.produce(arg.get("flow"), arg.get("msg"))

    def _setupStreamingProducer(self):
        def onStatistics(statistics):
            self._sendStatistics(statistics)

        self.messageProducer = MessageProducer(self._producerConfig, self._childs, self.node)
        self.messageProducer.registerStatisticsListener(onStatistics)
        if (self._childs):
            self.messageProducer.start()


    def _sendStatistics(self, data):
        try:
            self._statistics_queue.put(data)
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
