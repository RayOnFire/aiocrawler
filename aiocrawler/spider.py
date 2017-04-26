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
import sys
from copy import deepcopy
from pprint import pprint
from urllib.parse import urlparse
from functools import partial

from aiocrawler.exceptions import *
from aiocrawler.logger import *

try:
    import asyncio_redis
except ImportError:
    print('Asyncio Redis is not installed. Try pip install asyncio_redis')

COMMON_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Safari/537.36 Edge/14.14393',
}

'''
Example: get_host('http://www.google.com') -> 'google.com'
         get_host('https://abc.facebook.com') -> 'facebook.com'
'''
def get_host(url):
    parse = urlparse(url)
    netloc = parse.netloc
    return '.'.join(netloc.split('.')[-2:])

class Handler(object):

    def __init__(self, name, type_, callback, no_wrapper=False):
        self.name = name
        self.type_ = type_
        self.callback = callback
        self.no_wrapper = no_wrapper

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

    def __init__(self, loop=None, sem=10, config={}, logger_class=None, db_name='log.db', headers=None,
                    init_url=False, store_url=False, timeout=60, timeout_retry=10):
        self.handlers = {}
        self.headers_host = {}
        self.sem = asyncio.Semaphore(sem)
        self.sem_number = sem
        self.session = aiohttp.ClientSession()
        self.url_queue_manager = mp.Manager()
        self.url_queue_map = {}
        self.url_queue_for_mp = self.url_queue_manager.Queue(self.sem_number * 2)
        self.log_queue = self.url_queue_manager.Queue()
        self.config = config
        self.logger_class = logger_class
        self.db_name = db_name
        self.urls = {} # use to store crawled url and date
        self.store_url = store_url
        self.url_in_loop = 0
        self.timeout = timeout
        self.timeout_retry = timeout_retry
        self.status_handler = {
            404: self.handle_404,
            'others': self.handle_others_status,
        }
        # TODO: add init_url method to avoid recrawl same urls (from custom database), method may be overridable.
        if headers:
            self.predefined_headers = headers

    '''Utilities
    Folowing methods are utility function
    '''
    def __get_time(self):
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


    def __get_headers(self, hostname):
        if not hostname in self.headers_host:
            self.headers_host[hostname] = deepcopy(COMMON_HEADERS)
            if hasattr(self, 'predefined_headers'):
                self.headers_host[hostname].update(self.predefined_headers)
        return self.headers_host[hostname]

    async def __log_interval(self):
        while True:
            await asyncio.sleep(5)
            print(self.__get_time())
            print('Current url in loop:', self.url_in_loop)
            print('Current tasks:', len(asyncio.Task.all_tasks()))
            print('Current queue:')
            print('url_queue_for_mp:', self.url_queue_for_mp.qsize())
            print('log_queue:', self.log_queue.qsize())
            for (k, v) in self.url_queue_map.items():
                print(k, v.qsize())
            print('-------------------------------------------------------')

    '''Methods as a timing task
    '''
    async def __check_future_exception(self):
        while True:
            await asyncio.sleep(0.1)
            current_tasks = asyncio.Task.all_tasks()
            for task in current_tasks:
                if task.done():
                    print(task.exception())

    async def __check_url_queue(self):
        while True:
            await asyncio.sleep(0.1)
            if not self.url_queue_for_mp.empty():
                while True:
                    if self.url_in_loop > self.sem_number * 2:
                        break
                    try:
                        item = self.url_queue_for_mp.get_nowait() # use get_nowait to avoid block event loop
                        self.add_url(item['url'], item['handler'], item['options'])
                    except queue.Empty:
                        break

    '''Methods for fetching url
    '''
    async def __bound_fetch(self, url, handler_name, options, proxy=None):
        # TODO: Cause race condition here?
        if url in self.urls:
            self.log(meta='logger_info', args=4, time=self.__get_time(), type_='DUPLICATE_URL', url=url)
            return
        else:
            # avoid other corotinue fetch same url
            if self.store_url:
                self.urls[url] = None
        async with self.sem:
            await self.__fetch(url, handler_name, options, proxy)

    async def __fetch(self, url, handler_name, options, proxy=None, retry_time=0):
        options = deepcopy(options)
        options.update({'url': url})
        headers = self.__get_headers(get_host(url))
        response = asyncio.ensure_future(self.session.request('GET', url, headers=headers, proxy=self.config.get('proxy', None), timeout=self.timeout))
        response.add_done_callback(partial(self.__response_done_handler, url, headers, handler_name, options, retry_time))

    def __response_done_handler(self, url, headers, handler_name, options, retry_time, future):
        try:
            asyncio.ensure_future(self.__handle_response(future.result(), url, headers, handler_name, options))
        except concurrent.futures._base.TimeoutError:
            retry_time += 1
            if retry_time == self.timeout_retry:
                asyncio.ensure_future(self.__handle_bad_url(url, headers, handler_name, options, retry_time, reason='timeout'))
            else:
                asyncio.ensure_future(self.__fetch(url, handler_name, options, retry_time=retry_time))

    async def __handle_response(self, response, url, headers, handler_name, options):
        self.url_in_loop -= 1
        self.log(meta='logger_fetch', args=5, time=self.__get_time(), url=url, handler_name=handler_name, response_status=response.status)

        if response.status == 200:
            '''
            For future use to set url expire time
            WARNING: store url without expired time set may cause memory leakage when url increase rapidly or crawling for a long time
            TODO: add option to let subclass choose other database as url store e.g. Redis
            '''
            if self.store_url:
                self.urls[url] = self.__get_time()

            '''
            Automatically handle server-side returned 'Set-Cookie' header
            '''
            if 'Set-Cookie' in response.headers and not 'Cookie' in headers:
                print('set cookie:', response.headers['Set-Cookie'])
                headers.update({'Cookie': response.headers['Set-Cookie']})

            '''
            Raise NoHandlerError when handler not exists
            '''
            if not handler_name in self.handlers:
                raise NoHandlerError(handler_name)

            try:
                if self.handlers[handler_name].type_ == 'text':
                    item = await response.text()
                elif self.handlers[handler_name].type_ == 'binary':
                    item = await response.read()
                self.url_queue_map[handler_name].put({'response': item, 'options': options})
            except aiohttp.client_exceptions.ClientPayloadError:
                self.log(meta='logger_info', args=4, time=self.__get_time(), type_='CLIENTPAYLOADERROR', url=url)
                asyncio.ensure_future(self.refetch(url, handler_name, options))
            return True
        else:
            '''
            dispatcher according to response status code
            '''
            if response.status in self.status_handler:
                return not await self.status_handler[response.status](options)
            else:
                return not await self.status_handler['others'](options)

    async def __handle_bad_url(self, url, headers, handler_name, options, retry_time, reason):
        # TODO: inform user the url which can not be reached
        # print('bad url {0}'.format(url))
        pass

    async def __check_handlers_exception(self, executor_futures):
        while True:
            await asyncio.sleep(1)
            for f in executor_futures:
                if f.done():
                    print(f.exception())
                    rc = self.handle_child_process_exit(f)
                    return rc

    @staticmethod
    def process_wrapper(handler, item_queue, url_queue):
        while True:
            item = item_queue.get()
            handler(item, item_queue, url_queue)

    ''' Start entry
    This method is a crawler start entry.
    '''
    def start(self):
        self.log(meta='logger_info', args=4, time=self.__get_time(), type_='SPIDER_START')
        seprate_handlers = []
        loop = asyncio.get_event_loop()

        '''
        Retrieve handlers and create queue for each handler
        '''
        for k, v in self.handlers.items():
            seprate_handlers.append(v)
            self.url_queue_map[k] = self.url_queue_manager.Queue()

        if len(seprate_handlers) == 0:
            raise NoHandlerError

        '''
        Run logger in seperate process
        '''
        if self.logger_class:
            logger = self.logger_class(self.db_name)
            logger.start(self.log_queue)

        '''
        Run handlers in ProcessPoolExecutor as seperated processes
        '''
        with concurrent.futures.ProcessPoolExecutor(max_workers=len(seprate_handlers) + 1) as executor:
            executor_futures = []
            for handler in seprate_handlers:
                if handler.no_wrapper:
                    executor_futures.append(executor.submit(handler.callback, self.url_queue_map[handler.name], self.url_queue_for_mp))
                else:
                    executor_futures.append(executor.submit(self.process_wrapper, handler.callback, self.url_queue_map[handler.name], self.url_queue_for_mp))

            asyncio.ensure_future(self.__check_url_queue())
            #asyncio.ensure_future(self.__check_future_exception())
            #asyncio.ensure_future(self.__log_interval())
            
            rc = loop.run_until_complete(self.__check_handlers_exception(executor_futures))
            for proc in mp.active_children():
                proc.terminate()
            sys.exit(rc)

    '''Overridable
    Override following methods is allowed.
    '''
    def log(self, **kwargs):
        self.log_queue.put_nowait(kwargs)


    def handle_child_process_exit(future):
        return 1

    async def handle_others_status(self, info):
        return False

    async def handle_404(self, info):
        return False

    '''
    public methods
    '''
    def add_url(self, url, callback=None, options={}):
        self.url_in_loop += 1
        asyncio.ensure_future(self.__bound_fetch(url, callback, options))

    def register_callback(self, name, response_type, callback, no_wrapper=False, options=None):
        if name in self.handlers:
            raise DuplicateHandlerNameError(name)
        self.handlers[name] = Handler(name, response_type, callback, no_wrapper)

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

    async def handle_others_status(self, info):
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

    async def __check_handlers_exception(self, future):
        while True:
            await asyncio.sleep(1)
            if self.is_end:
                future.set_result('Done!')

    async def handle_response(self, response, url, headers, handler_name, options):
        await self.log('logger_url', json.dumps({'date': self.__get_time(), 'url': url, 'handler': handler_name, 'status': response.status}))
        return await super().handle_response(response, url, headers, handler_name, options)

    def stop(self):
        self.is_end = True

    def run(self):
        msg = {
            'date': self.__get_time(),
            'handlers': [{'name': v['name'], 'type': v['type']} for (k, v) in self.handlers.items()]
        }
        asyncio.ensure_future(self.log('logger_status', json.dumps(msg)))
        self.start()
