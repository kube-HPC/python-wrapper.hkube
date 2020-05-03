
from gevent import monkey
from tests.configs import config
from communication.DataRequest import DataRequest
from tests.mocks import mockdata
from util.encoding import Encoding

monkey.patch_all()
config = config.discovery

data1 = mockdata.dataTask1
data2 = mockdata.dataTask2
taskId1 = mockdata.taskId1
taskId2 = mockdata.taskId2
address1 = {'port': config['port'], 'host': config['host']}
address2 = {'port': "9021", 'host': config['host']}

encoding = Encoding(config['encoding'])


def test_get_data_bytes():
    dr = DataRequest({
        'address': address2,
        'taskId': taskId2,
        'dataPath': '',
        'encoding': config['encoding']
    })
    reply = dr.invoke()
    assert reply == data2


def test_get_data_by_path():
    dr = DataRequest({
        'address': address1,
        'taskId': taskId1,
        'dataPath': 'level1',
        'encoding': config['encoding']
    })
    reply = dr.invoke()
    assert reply == data1['level1']
    dr = DataRequest({
        'address': address1,
        'taskId': taskId1,
        'dataPath': 'value1',
        'encoding': config['encoding']
    })
    reply = dr.invoke()
    assert reply == data1['value1']


def test_path_not_exist():
    dr = DataRequest(
        {
            'address': address1,
            'taskId': taskId1,
            'dataPath': 'notExist',
            'encoding': config['encoding']
        })
    reply = dr.invoke()
    assert reply == {'error': {'code': 'noSuchDataPath', 'message': "notExist does not exist in data"}}


def test_get_complete_data():
    dr = DataRequest({
        'address': address1,
        'taskId': taskId1,
        'dataPath': '',
        'encoding': config['encoding']
    })
    reply = dr.invoke()
    assert reply == data1


def test_data_after_taskid_changed():

    dr = DataRequest({
        'address': address1,
        'taskId': taskId1,
        'dataPath': '',
        'encoding': config['encoding']
    })
    reply = dr.invoke()
    assert reply == data1
    dr = DataRequest({
        'address': address1,
        'taskId': taskId1,
        'dataPath': '',
        'encoding': config['encoding']
    })
    reply = dr.invoke()
    assert reply == data1


# def test_failing_to_get_data_old_task_id():

#     dr = DataRequest({
#         'address':address1,
#         'taskId': taskId1,
#         'dataPath': '',
#         'encoding': config['encoding']
#     })
#     reply = dr.invoke()
#     assert reply == data
#     dr = DataRequest({
#         'address':address1,
#         'taskId': taskId1,
#         'dataPath': '',
#         'encoding': config['encoding']
#     })
#     reply = dr.invoke()
#     assert reply == {'error': {'message': 'Current taskId is task_2', 'code': 'notAvailable'}}


# def test_failing_to_get_sending_ended():

#     dr = DataRequest({
#         'address':address1,
#         'taskId': taskId1,
#         'dataPath': '',
#         'encoding': config['encoding']
#     })
#     reply = dr.invoke()
#     assert reply == data
#     dr = DataRequest({
#         'address':address1,
#         'taskId': taskId1,
#         'dataPath': '',
#         'encoding': config['encoding']
#     })
#     reply = dr.invoke()
#     assert reply == {'error': {'message': 'Current taskId is None', 'code': 'notAvailable'}}


# def test_isServing():

#     def sleepNow(message):
#         gevent.sleep(3)
#         return reources['ds'].createReply(message)
#     reources['ds'].adpater.getReplyFunc = sleepNow
#     reources['ds'].setSendingState(taskId, data1)

#     dr = DataRequest(
#         {'address':address1, 'taskId': taskId1, 'dataPath': 'level1',
#          'encoding': 'bson'})
#     gevent.spawn(dr.invoke)
#     gevent.sleep(1)
#     assert reources['ds'].isServing() == True
#     gevent.sleep(3)
#     assert reources['ds'].isServing() == False


# @pytest.fixture(scope="function", autouse=True)
# def pytest_runtest_teardown(request):
#     def closeResource():
#         reources['ds'].close()

#     request.addfinalizer(closeResource)
