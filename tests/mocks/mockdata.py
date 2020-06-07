



taskId1 = 'task_1'
taskId2 = 'task_2'

dataTask1 = {
    'level1': {
        'level2': {
            'value1': 'd2_l1_l2_value_1',
            'value2': 'd2_l1_l2_value_2',
        },
        'value1': 'd2_l1_value_1'
    },
    'value1': 'd2_value_1'
}

dataTask2 = bytearray(b'\xdd'*(100))
dataTask2_original = bytearray(b'\xdd'*(100))

initData = {
    'jobId': 'jobId',
    'taskId': 'taskId',
    'input': [1, False, None],
    'nodeName': 'green',
    'spanId': {"uber-trace-id": "a0aa0bab5bfde7a6:7e187ec65fb04e0e:37bc77758a09b6a4:1"}
}


