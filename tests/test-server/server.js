const WebSocket = require('ws');
const http = require('http');
const { Encoding } = require('@hkube/encoding');
const PORT = process.env.PORT || '3000';
const encodingType = process.env.WORKER_ENCODING || 'bson';
const sleep = ms => new Promise(res => setTimeout(res, ms));

const encoding = new Encoding({ type: encodingType });

const array = [42, 37, 89, 95, 12, 126, 147];
const storageInfo1 = { data: { array } };
const storageInfo2 = { myValue: 'bla' };
const storageInfo3 = array;

const jobId = 'jobId-328901802';
const taskId = 'taskId-328901802';
const nodeName = 'green';

const input = [
    { data: '$$guid-1' },
    { prop: ['$$guid-2'] },
    [{ prop: '$$guid-3' }],
    ['$$guid-4'],
    '$$guid-5',
    '$$guid-6',
    '$$guid-7',
    'test-param',
    true,
    null,
    12345
];
const storage = {
    'guid-1': { storageInfo: storageInfo1, path: 'data.array' },
    'guid-2': { storageInfo: storageInfo2, path: 'myValue' },
    'guid-3': { storageInfo: storageInfo1, path: 'data.array', index: 3 },
    'guid-4': { storageInfo: storageInfo1, path: 'data.array', index: 0 },
    'guid-5': { storageInfo: storageInfo3, index: 2 },
    'guid-6': { storageInfo: storageInfo1 },
    'guid-7': [{ storageInfo: storageInfo1 }, { storageInfo: storageInfo2 }, { storageInfo: storageInfo3 }]
};

const data = {
    jobId,
    taskId,
    input,
    nodeName,
    storage,
    info: {
        savePaths: [nodeName]
    }
}

const main = async () => {
    const server = http.createServer();
    const wss = new WebSocket.Server({ server });
    wss.on('connection', (socket) => {
        console.log('connected');
        const send = message => socket.send(encoding.encode(message));
        socket.on('message', async data => {
            const payload = encoding.decode(data);
            console.log(`got command ${payload.command}`);
            let ret;
            switch (payload.command) {
                case 'initialized':
                    send({ command: 'start' })
                    break;
                case 'errorMessage':
                    console.log(`got error: ${JSON.stringify(payload)}`)
                    break;
                case 'startAlgorithmExecution':
                    const execId = '' + payload.data.execId
                    ret = `result from ${payload.data.algorithmName} execId: ${execId}`
                    send({ command: 'algorithmExecutionDone', data: { execId, response: ret } })
                    break;
                case 'startStoredSubPipeline':
                    const subPipelineId = '' + payload.data.subPipelineId
                    ret = `result from ${payload.data.subPipeline.name} subPipelineId: ${subPipelineId}`
                    send({ command: 'subPipelineDone', data: { subPipelineId, response: ret } })
                    break;
                case 'done':
                    console.log(`result: ${JSON.stringify(payload.data)}`)
                    break;
                default:
                    break;
            }
        })
        send({ command: 'initialize', data })

        socket.on('close', code => {
            console.log(`closed with code ${code}`)
        })
    })

    server.listen(PORT);
}

main()