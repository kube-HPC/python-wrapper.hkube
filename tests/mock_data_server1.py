

from communication.DataServer import DataServer
from tests.mocks import mockdata
from tests.configs import config
from util.encoding import Encoding

encoding = Encoding(config.discovery['encoding'])

ds = DataServer(config.discovery)
ds.setSendingState(mockdata.taskId1, encoding.encode(mockdata.dataTask1))
ds.listen()
