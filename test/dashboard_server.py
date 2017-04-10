import asyncio
import websockets
import asyncio_redis
import json
from pprint import pprint

async def dispatch(ws, path):
    redis_conn = await asyncio_redis.Connection.create(host='localhost', port=6379)
    while True:
        command = await ws.recv()
        command = json.loads(command)
        if command['type'] == 'getLoggerList':
            await send_logger_list(ws, redis_conn)
        elif command['type'] == 'subscribeLogger':
            await subscribe_logger(ws, redis_conn)
        elif command['type'] == 'getLogger':
            logger_type = await redis_conn.type(command['data'])
            if logger_type.status == 'set':
                items = []
                set_futures = await redis_conn.smembers(command['data'])
                for f in set_futures:
                    item = await f
                    items.append(item)
                await send_set(ws, redis_conn, command['data'], items)

async def send_set(ws, redis_conn, logger_name, items):
    await redis_conn.send(json.dumps({'type': 'loggerSet', 'loggerName': logger_name, 'data': items}))

async def send_logger_list(ws, redis_conn):
    cursor = await redis_conn.scan(match='logger_*')
    logger_list = await cursor.fetchall()
    print(logger_list)
    await ws.send(json.dumps({'type': 'loggerList', 'data': logger_list}))

# subscribe all channel start with 'logger_'
async def subscribe_logger(ws, redis_conn):
    subscriber = await redis_conn.start_subscribe()
    await subscriber.psubscribe(['logger_*'])
    while True:
        msg = await subscriber.next_published()
        await ws.send(json.dumps({'type': 'loggerLine', 'loggerName': msg.channel[7:], 'data': msg.value}))

start_server = websockets.serve(dispatch, 'localhost', 8765)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()