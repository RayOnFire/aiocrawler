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
import time
import signal
import os
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
    """ The generic spider with basic function

    This class have the basic function for crawling web.
    (1) Logging: This class logged with sqlite3 database, and create two tables named `logger_info` and `logger_fetch`.
                 The first one is for storing spider metadata e.g. start of spider, exception in processing.
                 The last one is for storing crawled urls, which can be useful for starting from previous point of process.
    (2) Multiprocessing: As known to us, Python employed GIL for simplify multithreading, so in order to run spider on a
                         multicore system more effetively,we need multiprocessing. The basic idea is let event loop run in
                         main process, and let response handlers run in seperate process to avoid blocking event loop.
                         And queue was used to communicate between processes. In order to inform child process the exit of
                         main process, main process send SIGINT signal to every child process, which can be useful for child
                         processes to do some clean up job.
    """

    def __init__(self, loop=None, sem=10, config={}, db_name='log.db', headers=None):
        self.handlers = {}
        self.headers_host = {}
        self.sem = asyncio.Semaphore(sem)
        self.is_end = False
        self.session = aiohttp.ClientSession()
        self.url_queue_manager = mp.Manager()
        self.url_queue_map = {}
        self.url_queue_for_mp = self.url_queue_manager.Queue(50)
        self.log_queue = self.url_queue_manager.Queue()
        self.config = config
        self.db_name = db_name
        self.urls = {} # use to store crawled url and date
        self.conn = sqlite3.connect(db_name)
        self.cur = self.conn.cursor()
        self.url_in_loop = 0
        self.status_handler = {
            400: self.handle_400,
            404: self.handle_404,
            500: self.handle_500,
            'others': self.handle_others,
        }
        self._init_db()
        self._init_url()
        if headers:
            self.headers = headers

    def _init_url(self):
        try:
            self.cur.execute("SELECT DISTINCT url FROM logger_fetch")
            urls = self.cur.fetchall()
            for url in urls:
                self.urls[url] = None
        except sqlite3.OperationalError:
            pass

    def _init_db(self):
        self.conn.execute(("CREATE TABLE IF NOT EXISTS logger_fetch ("
                             "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                             "date TEXT,"
                             "url TEXT NOT NULL,"
                             "handler_name TEXT,"
                             "status INTEGER NOT NULL)"))
        self.conn.execute(("CREATE TABLE IF NOT EXISTS logger_info ("
                            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                            "date TEXT,"
                            "type TEXT NOT NULL,"
                            "message TEXT)"))
        self.conn.commit()

    def _get_time(self):
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def register_callback(self, name, response_type, callback, run_in_process=False, options=None, no_wrapper=False):
        if name in self.handlers:
            # TODO: raise error here
            print('handler name should be unique')
            return
        self.handlers[name] = {'name': name, 'type': response_type, 'callback': callback, 'run_in_process': run_in_process, 'no_wrapper': no_wrapper}

    def _get_headers(self, hostname):
        if hasattr(self, 'headers'):
            return self.headers
        if hostname in self.headers_host:
            return self.headers_host[hostname]
        else:
            self.headers_host[hostname] = deepcopy(COMMON_HEADERS)
            return self.headers_host[hostname]

    async def log_interval(self):
        while True:
            await asyncio.sleep(5)
            print(self._get_time())
            print('Current url in loop:', self.url_in_loop)
            print('Current tasks:', self.Task.all_tasks())
            print('Current queue:')
            print('url_queue_for_mp:', self.url_queue_for_mp.qsize())
            print('log_queue:', self.log_queue.qsize())
            for (k, v) in self.url_queue_map.items():
                print(k, v.qsize())
            print('-------------------------------------------------------')

    def add_url(self, url, callback=None, options={}):
        self.url_in_loop += 1
        task = asyncio.ensure_future(self.bound_fetch(url, callback, options))
        #print(task)

    async def check_queue(self):
        while True:
            await asyncio.sleep(0.1)
            if not self.url_queue_for_mp.empty():
                while True:
                    if self.url_in_loop > 100:
                        break
                    try:
                        item = self.url_queue_for_mp.get_nowait() # use get_nowait to avoid block event loop
                        self.add_url(item['url'], item['handler'], item['options'])
                    except queue.Empty:
                        break

    async def bound_fetch(self, url, handler_name, options, proxy=None):
        # TODO: Cause race condition here?
        if url in self.urls:
            self.log_queue.put_nowait({'table': 'logger_info', 'args': 4, 'data': (None, self._get_time(), 'DUPLICATE_URL', url)})
            #self.conn.commit()
            return
        else:
            # avoid other corotinue fetch same url
            self.urls[url] = None
        async with self.sem:
            await self.fetch(url, handler_name, options, proxy)

    async def fetch(self, url, handler_name, options, proxy=None):
        o = deepcopy(options)
        o.update({'url': url})
        while True:
            headers = self._get_headers(get_host(url))
            try:
                response = await self.session.request('GET', url, headers=headers, proxy=self.config.get('proxy', None))
                if await self.handle_response(response, url, headers, handler_name, o):
                    break
            except aiohttp.client_exceptions.ServerDisconnectedError:
                #asyncio.ensure_future(self.refetch(url, handler_name, options))
                self.log_queue.put_nowait({'table': 'logger_info', 'args': 4, 'data': (None, self._get_time(), 'SERVER_DISCONNECT_EXCEPTION', url)})
                #self.conn.commit()

    async def refetch(self, url, handler_name, options, proxy=None):
        del self.urls[url]
        await self.bound_fetch(url, handler_name, options, proxy)

    async def handle_response(self, response, url, headers, handler_name, options):
        self.url_in_loop -= 1
        self.log_queue.put_nowait({'table': 'logger_fetch', 'args': 5, 'data': (None, self._get_time(), url, handler_name, response.status)})
        if response.status == 200:
            self.urls[url] = self._get_time()  # For future use to set url expire time
            if 'Set-Cookie' in response.headers and not 'Cookie' in headers:
                print('set cookie:', response.headers['Set-Cookie'])
                headers.update({'Cookie': response.headers['Set-Cookie']})
            if not handler_name in self.handlers:
                if handler_name != None:
                    # TODO: handler not exist, raise error
                    return True
            try:
                if self.handlers[handler_name]['run_in_process']:
                    if self.handlers[handler_name]['type'] == 'text':
                        item = await response.text()
                    elif self.handlers[handler_name]['type'] == 'binary':
                        item = await response.read()
                    self.url_queue_map[handler_name].put({'response': item, 'options': options})
                else:
                    if self.handlers[handler_name]['type'] == 'binary':
                        item = await response.binary()
                    elif self.handlers[handler_name]['type'] == 'text':
                        item = await response.text()
                    self.handlers[handler_name]['callback'](item, self, options)
            except aiohttp.client_exceptions.ClientPayloadError:
                self.log_queue.put_nowait({'table': 'logger_info', 'args': 4, 'data': (None, self._get_time(), 'CLIENTPAYLOADERROR', url)})
                asyncio.ensure_future(self.refetch(url, handler_name, options))
            return True
        else:
            if response.status in self.status_handler:
                return not await self.status_handler[response.status](options)
            else:
                return not await self.status_handler['others'](options)

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

    @staticmethod
    def logger_process(db_name, conn, log_queue):
        conn = sqlite3.connect(db_name)
        while True:
            log = log_queue.get()
            sql = 'INSERT INTO ' + log['table'] + ' VALUES (' + ('?, ' * log['args'])[:-2] + ')'
            conn.execute(sql, log['data'])
            conn.commit() # TODO: use timer signal to commit?

    def stop(self):
        self.is_end = True

    def start(self):
        self.log_queue.put_nowait({'table': 'logger_info', 'args': 4, 'data': (None, self._get_time(), 'SPIDER_START', None)})
        seprate_handlers = []
        loop = asyncio.get_event_loop()

        for k, v in self.handlers.items():
            if v['run_in_process']:
                seprate_handlers.append(v)
                self.url_queue_map[k] = self.url_queue_manager.Queue()
        if len(seprate_handlers) > 0:
            with concurrent.futures.ProcessPoolExecutor(max_workers=len(seprate_handlers) + 1) as executor:
                loop.run_in_executor(executor, self.logger_process, self.db_name, self.conn, self.log_queue)
                for h in seprate_handlers:
                    if h['no_wrapper']:
                        loop.run_in_executor(executor, h['callback'], self.url_queue_map[h['name']], self.url_queue_for_mp)
                    else:
                        loop.run_in_executor(executor, self.process_wrapper, h['callback'], self.url_queue_map[h['name']], self.url_queue_for_mp)

                # Check flag and keep event loop
                f = asyncio.Future()
                asyncio.ensure_future(self.check_end(f))
                asyncio.ensure_future(self.check_queue())
                asyncio.ensure_future(self.log_interval())
                try:
                    loop.run_until_complete(f)
                except Exception as e:
                    # retrieve tasks to avoid exception show in console
                    #print(e.message)
                    tasks = asyncio.Task.all_tasks()
                    # close db connection
                    self.conn.commit()
                    #self.conn.close()
                    # use SIGINT to inform child process to exit.
                    for p in mp.active_children():
                        os.kill(p.pid, signal.SIGINT)

        else:
            f = asyncio.Future()
            asyncio.ensure_future(self.check_end(f))
            asyncio.ensure_future(self.check_queue())
            try:
                loop.run_until_complete(f)
            except KeyboardInterrupt:
                self.conn.commit()
                raise KeyboardInterrupt

    def run(self):
        self.start()

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
