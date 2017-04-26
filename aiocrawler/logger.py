import multiprocessing as mp
import sqlite3
import signal
import sys
import asyncio
from functools import partial

class Sqlite3Logger(object):

    def __init__(self, db_name):
        self.db_name = db_name
        self._init_db()

    def _init_db(self):
        self.conn = sqlite3.connect(self.db_name)
        self.cur = self.conn.cursor()
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

    @staticmethod
    def logger_process(db_name, log_queue):
        def on_sigterm(conn, sig, frame):
            conn.commit()
            sys.exit(0)
        # TODO: register handler for SIGTERM
        async def commit(conn):
            while True:
                await asyncio.sleep(5)
                conn.commit()

        async def get_log(queue, cursor):
            while True:
                await asyncio.sleep(0.1)
                if not queue.empty():
                    log = queue.get_nowait()
                    sql = 'INSERT INTO ' + log['meta'] + ' VALUES (' + ('?, ' * log['args'])[:-2] + ')'
                    if log['meta'] == 'logger_fetch':
                        conn.execute(sql, (None, log['time'], log['url'], log['handler_name'], log['response_status']))

        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        signal.signal(signal.SIGTERM, partial(on_sigterm, conn))
        loop = asyncio.get_event_loop()
        asyncio.ensure_future(commit(conn))
        asyncio.ensure_future(get_log(log_queue, cursor))
        loop.run_forever()

    def start(self, log_queue):
        p = mp.Process(target=self.logger_process, args=(self.db_name, log_queue))
        p.start()