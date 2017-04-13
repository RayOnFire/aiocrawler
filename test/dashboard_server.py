import asyncio
import websockets
import asyncio_redis
import json
import psutil
import subprocess
from pprint import pprint

async def dispatch(ws, path):
    redis_conn = await asyncio_redis.Connection.create(host='localhost', port=6379)
    while True:
        command = await ws.recv()
        asyncio.ensure_future(send_server_status(ws))
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
        elif command['type'] == 'init':
            # send init log from local sqlite db
            pass

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

async def send_server_status(ws):
    while True:
        await asyncio.sleep(1)
        cpu_usage = psutil.cpu_percent()
        memory_usage = psutil.virtual_memory().percent
        await ws.send(json.dumps({'type': 'serverStatus', 'cpuUsage': cpu_usage, 'memoryUsage': memory_usage, 'topProc': get_proc()}))

def get_proc():
    proc = []
    with subprocess.Popen(['top', '-b', '-n1'], stdout=subprocess.PIPE) as p:
        for line in p.stdout:
            l = line.decode().split()
            if 'top' in l:
                continue
            proc.append(l)
    return [{'pid': i[0], 'cpu': i[8], 'memory': i[9], 'name': i[-1]} for i in proc[7:12]]

start_server = websockets.serve(dispatch, 'localhost', 8765)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()