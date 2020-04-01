
import gevent
from communication.DataServer import DataServer
from tests.mocks import mockdata
from tests.configs import config


ds = DataServer(config.discovery)
ds.setSendingState(mockdata.taskId, mockdata.data)
ds.listen()
