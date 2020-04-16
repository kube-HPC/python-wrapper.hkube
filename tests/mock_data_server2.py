

from communication.DataServer import DataServer
from tests.mocks import mockdata
from tests.configs import config
from util.encoding import Encoding

encoding = Encoding(config.discovery['encoding'])

config.discovery.update({"port": "9021"})
ds = DataServer(config.discovery)
ds.setSendingState(mockdata.taskId2, encoding.encode(mockdata.dataTask2))
ds.listen()
