import asyncio
import aiohttp
import logging
import time
import concurrent.futures
import queue
import multiprocessing as mp
import datetime
import json
import sqlite3
from copy import deepcopy
from pprint import pprint

try:
    import asyncio_redis
except ImportError:
    print('Asyncio Redis is not installed. Try pip install asyncio_redis')

COMMON_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Safari/537.36 Edge/14.14393',
}

def get_host(url):
    if url.startswith('https'):
        u = url.replace('https://', '')
    elif url.startswith('http'):
        u = url.replace('http://', '')
    return u.split('/')[0]

class BaseSpider(object):

    def __init__(self, loop=None, sem=10, config=None):
        self.handlers = {}
        self.headers_host = {}
        self.sem = asyncio.Semaphore(sem)
        self.is_end = False
        self.session = aiohttp.ClientSession()
        self.url_queue_manager = mp.Manager()
        self.url_queue_map = {}
        self.url_queue_for_mp = self.url_queue_manager.Queue()
        self.config = config

    def _get_time(self):
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def register_callback(self, name, response_type, callback, run_in_process=False, options=None):
        if name in self.handlers:
            # TODO: raise error here
            print('handler name should be unique')
            return
        if run_in_process:
            self.handlers[name] = {'name': name, 'type': response_type, 'callback': callback, 'run_in_process': True}
        else:
            self.handlers[name] = {'name': name, 'type': response_type, 'callback': callback}

    def _get_headers(self, hostname):
        if hostname in self.headers_host:
            return self.headers_host[hostname]
        else:
            self.headers_host[hostname] = deepcopy(COMMON_HEADERS)
            return self.headers_host[hostname]

    def add_url(self, url, callback, options={}):
        asyncio.ensure_future(self.bound_fetch(url, callback, options))

    async def check_queue(self):
        while True:
            await asyncio.sleep(0.5)
            if not self.url_queue_for_mp.empty():
                while True:
                    try:
                        item = self.url_queue_for_mp.get_nowait()
                        self.add_url(item['url'], item['handler'], item['options'])
                    except queue.Empty:
                        break

    async def bound_fetch(self, url, handler_name, options, proxy=None):
        async with self.sem:
            await self.fetch(url, handler_name, options, proxy)

    async def fetch(self, url, handler_name, options, proxy=None):
        o = deepcopy(options)
        o.update({'url': url})
        while True:
            headers = self._get_headers(get_host(url))
            async with self.session.request('GET', url, headers=headers, proxy=self.config.get('proxy', None)) as response:
                if await self.handle_response(response, url, headers, handler_name, o):
                    break

    async def handle_response(self, response, url, headers, handler_name, options):
        if response.status == 200:
            if 'Set-Cookie' in response.headers and not 'Cookie' in headers:
                print('set cookie:', response.headers['Set-Cookie'])
                headers.update({'Cookie': response.headers['Set-Cookie']})
            if not handler_name in self.handlers:
                # TODO: handler not exist, raise error
                return True
            if 'run_in_process' in self.handlers[handler_name]:
                if self.handlers[handler_name]['type'] == 'text':
                    self.url_queue_map[handler_name].put({'response': await response.text(), 'options': options})
                elif self.handlers[handler_name]['type'] == 'binary':
                    self.url_queue_map[handler_name].put({'response': await response.read(), 'options': options})
            else:
                if self.handlers[handler_name]['type'] == 'binary':
                    self.handlers[handler_name]['callback'](await response.read(), self, options)
                elif self.handlers[handler_name]['type'] == 'text':
                    self.handlers[handler_name]['callback'](await response.text(), self, options)
            return True
        elif response.status == 400:
            return not await self.handle_400(options)
        elif response.status == 404:
            return not await self.handle_404(options)
        elif response.status == 500:
            return not await self.handle_500(options)
        else:
            return not await self.handle_others(options)

    async def handle_others(self, info):
        return False

    async def handle_404(self, info):
        return False

    async def handle_400(self, info):
        return False

    async def handle_500(self, info):
        return False

    async def check_end(self, future):
        while True:
            await asyncio.sleep(1)
            if self.is_end:
                future.set_result('Done!')

    @staticmethod
    def process_wrapper(handler, item_queue, url_queue):
        while True:
            item = item_queue.get()
            handler(item, item_queue, url_queue)

    def stop(self):
        self.is_end = True

    def start(self):
        seprate_handlers = []
        loop = asyncio.get_event_loop()

        for k, v in self.handlers.items():
            if 'run_in_process' in v:
                seprate_handlers.append(v)
                self.url_queue_map[k] = self.url_queue_manager.Queue()
        with concurrent.futures.ProcessPoolExecutor(max_workers=len(seprate_handlers)) as executor:
            for h in seprate_handlers:
                loop.run_in_executor(executor, self.process_wrapper, h['callback'], self.url_queue_map[h['name']], self.url_queue_for_mp)

            # Check flag and keep event loop
            f = asyncio.Future()
            asyncio.ensure_future(self.check_end(f))
            asyncio.ensure_future(self.check_queue())
            loop.run_until_complete(f)

class RedisSpider(BaseSpider):

    def __init__(self, loop=None, sem=10, config=None):
        super().__init__(loop, sem, config)
        self.retry_threshold = 10
        self.status_500_count_lock = asyncio.Lock()
        self.status_500_count = 0
        self.status_500_threshold = 50
        self.sleep_time = 10
        self.config = config
        self.session = aiohttp.ClientSession()
        self.retry_count = {}
        self.redis_conn = None
        asyncio.ensure_future(self.create_redis())

    async def create_redis(self):
        self.redis_conn = await asyncio_redis.Connection.create(host='localhost', port=6379)

    def add_redis_logger(name):
        pass

    async def handle_others(self, info):
        await self.log('logger_other_status', json.dumps(info))
        return False

    async def handle_404(self, info):
        await self.log('logger_404', json.dumps(info))
        return False

    async def handle_400(self, info):
        await self.log('logger_400', json.dumps(info))
        return False

    async def handle_500(self, info):
        # TODO: add threshold for stop asyncio
        await self.log('logger_500', json.dumps(info))
        return True

    async def log(self, logger_name, data):
        while not self.redis_conn:
            await asyncio.sleep(0.1)
        await self.redis_conn.publish(logger_name, data)

    async def check_end(self, future):
        while True:
            await asyncio.sleep(1)
            if self.is_end:
                future.set_result('Done!')

    async def handle_response(self, response, url, headers, handler_name, options):
        await self.log('logger_url', json.dumps({'date': self._get_time(), 'url': url, 'handler': handler_name, 'status': response.status}))
        return await super().handle_response(response, url, headers, handler_name, options)

    def stop(self):
        self.is_end = True

    def run(self):
        msg = {
            'date': self._get_time(),
            'handlers': [{'name': v['name'], 'type': v['type']} for (k, v) in self.handlers.items()]
        }
        asyncio.ensure_future(self.log('logger_status', json.dumps(msg)))
        self.start()

class SQLiteSpider(BaseSpider):

    def __init__(self, loop=None, sem=10, config=None, db='log.db'):
        super().__init__(loop, sem, config)
        self.db_name = db

if __name__ == '__main__':
    def handler(response, options):
        print(response)
    s = Spider()
    s.register_callback('h1', 'text', handler)
    s.add_url('http://www.baidu.com', 'h1')
    s.run()
    print(s.handlers)
