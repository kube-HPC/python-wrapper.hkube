const WebSocket = require('ws');
const http = require('http');
const msgpack = require('msgpack5')();
const PORT = process.env.PORT || '3000';
const sleep = ms => new Promise(res => setTimeout(res, ms));
const log = console.log
console.log = (...args) => { log(new Date(), ...args) }
const length=100
const toSend = new Uint8Array(length).fill(1);
const main = async () => {
    const server = http.createServer();
    const wss = new WebSocket.Server({ server, maxPayload:500e6 });
    wss.on('connection', socket => {
        console.log('connected');
        const send = message => {
            console.log(`sending command ${message.command}`)
            socket.send(msgpack.encode(message))
            // socket.send(JSON.stringify(message))

        };
        socket.on('message', async data => {
            console.log(`got message`);
            const payload = msgpack.decode(data);
            if (payload.command && typeof payload.command === 'Buffer') {
                payload.command = payload.command.toString();
            }
            // const payload = JSON.parse(data);

            console.log(`got command ${payload.command}`);
            let ret;
            switch (payload.command) {
                case 'initialized':
                    send({ command: 'start' })
                    break;
                case 'startAlgorithmExecution':
                    console.log(payload.data.execId)
                    if (payload.data.execId == 70) {
                        send({ command: 'stop' })
                        await sleep(500)
                    }
                    const execId = '' + payload.data.execId
                    ret = `result from ${payload.data.algorithmName} execId: ${execId}`
                    // await sleep(500)
                    send({ command: 'algorithmExecutionDone', data: { execId, response: payload.data.input[0] } })
                    break;
                case 'startStoredSubPipeline':
                    const subPipelineId = '' + payload.data.subPipelineId
                    ret = `result from ${payload.data.subPipeline.name} subPipelineId: ${subPipelineId}`
                    send({ command: 'subPipelineDone', data: { subPipelineId, response: ret } })
                    break;
                case 'done':
                    console.log(`got result: ${JSON.stringify(payload)}`)
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