#!/usr/bin/env python

# WS server example

import asyncio
import websockets
import simplejson as json


async def worker(websocket, path):
    print('connected')
    msg = {'command': 'initialize', 'input': []}
    await websocket.send(json.dumps(msg))

    async for message in websocket:
        parsed = json.loads(message)
        command = parsed["command"]
        data = parsed["data"]
        if command == "initialized":
            msg = {'command': 'start'}
            await websocket.send(json.dumps(msg))
        if command == "done":
            print('done')
            print(message)

        if command == "startAlgorithmExecution":
            execId = data['execId']
            if execId == 1:
                await asyncio.sleep(3)
                msg = {'command': 'algorithmExecutionDone', 'data': {
                    'execId': execId, 'response': 'foo'}}
            else:
                await asyncio.sleep(1)
                msg = {'command': 'algorithmExecutionDone', 'data': {
                    'execId': execId, 'error':'bar'}}
            await websocket.send(json.dumps(msg))


start_server = websockets.serve(worker, "localhost", 5555)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()
