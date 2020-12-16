from hkube_python_wrapper.communication.zmq.streaming.producer.MessageQueue import MessageQueue
flow1 =   [{'source': 'B', 'next': ['C']}, {'source': 'C', 'next': ['D']}]
flow2 =  [{'source': 'B', 'next': ['A', 'C']}, {'source': 'C', 'next': ['D']},{'source': 'D', 'next': ['E']}]

def test_queue_statistics():
    queue2 = MessageQueue(['A','C'],'B')
    queue2.append(flow2,'header1','message1')
    queue2.append(flow2, 'header2', 'message2')
    queue2.append(flow2, 'header3', 'message3')
    queue2.append(flow1, 'header4', 'message4')
    assert queue2.size('A') == 3
    queue2.pop('A')
    assert queue2.size('A') == 2
    assert queue2.size('C') == 4
    assert queue2.sent['A'] == 1
    assert queue2.sent['C'] == 0
    queue2.pop('A')
    queue2.pop('A')
    queue2.pop('A')
    assert queue2.sizeSum != 0
    queue2.pop('C')
    queue2.pop('C')
    queue2.pop('C')
    queue2.pop('C')
    assert queue2.size('A') == 0
    assert queue2.size('C') == 0
    assert queue2.sent['A'] == 3
    assert queue2.sent['C'] == 4
    assert queue2.sizeSum != 0
