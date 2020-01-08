const WebSocket = require('ws');
const http = require('http');
const PORT = process.env.PORT || '3000';
const binary = (process.env.WORKER_BINARY==='true')
const bson = require('bson');
const sleep = ms => new Promise(res => setTimeout(res, ms));

const parse = binary?(data)=>bson.deserialize(data, { promoteBuffers: true, promoteValues: true }):JSON.parse
const stringify = binary?bson.serialize:JSON.stringify
const main = async () => {
    const server = http.createServer();
    const wss = new WebSocket.Server({ server });
    wss.on('connection', socket => {
        console.log('connected');
        const send = message => socket.send(stringify(message));
        socket.on('message', async data => {
            const payload = parse(data);
            console.log(`got command ${payload.command}`);
            let ret;
            switch (payload.command) {
                case 'initialized':
                    send({ command: 'start' })
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
        send({ command: 'initialize', data: { input: ['eval-alg'] } })

        socket.on('close', code => {
            console.log(`closed with code ${code}`)
        })
    })



    server.listen(PORT);
}

main()