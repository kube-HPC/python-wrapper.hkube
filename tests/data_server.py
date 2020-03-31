

from communication.DataServer import DataServer
import tests.configs.config as conf
config = conf.Config

taskId = 'task_1'
data = {
    'level1': {
        'level2': {
            'value1': 'd2_l1_l2_value_1',
            'value2': 'd2_l1_l2_value_2',
        },
        'value1': 'd2_l1_value_1'
    },
    'value1': 'd2_value_1'
}

ds = DataServer(config.discovery)
ds.setSendingState(taskId, data)
ds.listen()
