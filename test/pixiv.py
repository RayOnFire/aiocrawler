from aiocrawler.spider import BaseSpider
import sqlite3
import multiprocessing as mp
import signal
import os
import sys
import psutil
from functools import partial

h_pixiv = {
    'App-OS': 'ios',
    'App-OS-Version': '10.3.1',
    'App-Version': '6.7.1',
    'User-Agent': 'PixivIOSApp/6.7.1 (iOS 10.3.1; iPhone8,1)'
}

def init_db():
    conn = sqlite3.connect('pixiv3.db')
    conn.execute(("CREATE TABLE IF NOT EXISTS pixiv ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                    "url TEXT NOT NULL UNIQUE,"
                    "info TEXT NOT NULL)"))
    conn.commit()
    conn.close()

def stdout(s):
    if type(s) != str:
        s = str(s)
    sys.stdout.write(s + '\n')
    sys.stdout.flush()

def add_to_database(queue: mp.Queue, url_queue: mp.Queue) -> None:
    #def on_terminate(sig, frame):
    #   conn.commit()
    #   conn.close()
    #   print('committed!')
    #   os.kill(mp.current_process().pid, signal.SIGTERM)
    #signal.signal(signal.SIGINT, on_terminate)
    def commit(p_entry, sig, frame):
        conn.commit()
        stdout('pixiv commit!' + ' ' + 'last entry:' + ' ' + str(p_entry[1]) + ' ' + 'previous entry:' + ' ' + str(p_entry[0]))
        if p_entry[1] != 0:
            if p_entry[1] != p_entry[0]:
                p_entry[0] = p_entry[1]
            else:
                stdout('equal')
                for pid in psutil.pids():
                    p = psutil.Process(pid)
                    if p.name() == 'python':
                        stdout(pid)
                        os.kill(pid, signal.SIGKILL)
        
    p_entry = [-1, 0]
    signal.setitimer(signal.ITIMER_REAL, 5, 5)
    signal.signal(signal.SIGALRM, partial(commit, p_entry))
    conn = sqlite3.connect('pixiv3.db')
    while True:
        item = queue.get()
        try:
            conn.execute("INSERT INTO pixiv VALUES (?, ?, ?)", (None, item['options']['url'], item['response']))
            #conn.commit()
            p_entry[1] = item['options']['url'][-8:]
        except:
            pass


def url_adder(url_queue: mp.Queue, low, hign) -> None:
    try:
        for i in range(low, high):
            o = {
                'url': 'https://app-api.pixiv.net/v1/illust/detail?illust_id=' + str(i),
                'handler': 'add_to_database',
                'options': {},
            }
            #print(i)
            url_queue.put(o, True, None)
    except KeyboardInterrupt:
        print('url_adder exit!')

#config={'proxy': 'http://127.0.0.1:1080'}

if __name__ == '__main__':
    low = int(sys.argv[1])
    high = int(sys.argv[2])
    init_db()
    pixiv_spider = BaseSpider(db_name='log2.db', headers=h_pixiv, sem=50)
    pixiv_spider.register_callback('add_to_database', 'text', add_to_database, run_in_process=True, no_wrapper=True)
    p = mp.Process(target=url_adder, args=(pixiv_spider.url_queue_for_mp, low, high), name='url_adder')
    p.start()
    #for i in range(10000):
    #   pixiv_spider.add_url('https://app-api.pixiv.net/v1/illust/detail?illust_id=' + str(i), 'add_to_database')
    pixiv_spider.run()