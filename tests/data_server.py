

import tests.mock_data.mocks as mocks
from communication.DataServer import DataServer
import tests.configs.config as conf
config = conf.Config


ds = DataServer(config.discovery)
ds.setSendingState(mocks.taskId, mocks.data)
ds.listen()
