from aiocrawler.spider import BaseSpider
from aiocrawler.exceptions import NotStartError, ProcessStopError
import sqlite3
import multiprocessing as mp
import signal
import os
import sys
import psutil
import json
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
                        "id INTERGER PRIMARY KEY,"
                        "create_date TEXT,"
                        "height INTEGER,"
                        "width INTEGER,"
                        "url TEXT,"
                        "title TEXT,"
                        "comment_count INTEGER,"
                        "view_count INTEGER,"
                        "bookmark_count INTEGER,"
                        "username TEXT)"))

    conn.execute(("CREATE TABLE IF NOT EXISTS tag ("
                        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                        "name TEXT NOT NULL UNIQUE)"))

    conn.execute(("CREATE TABLE IF NOT EXISTS pixiv_tag ("
                        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                        "pixiv_id INTEGER,"
                        "tag_id INTEGER,"
                        "FOREIGN KEY (pixiv_id) REFERENCES pixiv(id),"
                        "FOREIGN KEY (tag_id) REFERENCES tag(id))"))

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
        if p_entry[2] == 5:
            raise NotStartError
        conn.commit()
        stdout('pixiv commit!' + ' ' + 'last entry:' + ' ' + str(p_entry[1]) + ' ' + 'previous entry:' + ' ' + str(p_entry[0]))
        if p_entry[1] != 0:
            if p_entry[1] != p_entry[0]:
                p_entry[0] = p_entry[1]
            else:
                raise ProcessStopError
        else:
            p_entry[2] += 1
        
    p_entry = [-1, 0, 0]
    signal.setitimer(signal.ITIMER_REAL, 5, 5)
    signal.signal(signal.SIGALRM, partial(commit, p_entry))
    conn = sqlite3.connect('pixiv3.db')
    cursor = conn.cursor()
    while True:
        stdout('add_to_database start')
        item = queue.get()
        #stdout(item)
        obj = json.loads(item['response'])
        #stdout(obj)
        tag_ids = []
        for tag in obj['illust']['tags']:
            cursor.execute("SELECT id FROM tag WHERE name=?", (tag['name'],))
            tag_id = cursor.fetchone()
            if tag_id:
                tag_ids.append(tag_id[0])
            else:
                cursor.execute("INSERT INTO tag VALUES (null, ?)", (tag['name'],))
                cursor.execute("SELECT id FROM tag WHERE name=?", (tag['name'],))
                tag_id = cursor.fetchone()
                tag_ids.append(tag_id[0])
        obj = obj['illust']
        data = (obj['id'], obj['create_date'], obj['height'], obj['width'], obj['meta_single_page'].get('original_image_url', None), obj['title'], obj['total_comments'], obj['total_view'], obj['total_bookmarks'], obj['user']['account'])
        cursor.execute("INSERT INTO pixiv VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", data)
        for tag_id in tag_ids:
            cursor.execute("INSERT INTO pixiv_tag VALUES (null, ?, ?)", (obj['id'], tag_id))
        '''
        conn.execute("INSERT INTO pixiv VALUES (?, ?, ?)", (None, item['options']['url'], item['response']))
        #conn.commit()
        '''
        p_entry[1] = item['options']['url'][-8:]


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
class PixivSpider(BaseSpider):

    def handle_child_process_exit(self, future):
        ex = future.exception()
        if isinstance(ex, NotStartError):
            print('NotStartError')
            return 2
        elif isinstance(ex, ProcessStopError):
            print('ProcessStopError')
            return 3

if __name__ == '__main__':
    low = int(sys.argv[1])
    high = int(sys.argv[2])
    init_db()
    if len(sys.argv) > 3 and sys.argv[3] == 'proxy':
        config={'proxy': 'http://127.0.0.1:1080'}
    else:
        config = {}
    pixiv_spider = PixivSpider(config=config, db_name='log2.db', headers=h_pixiv, sem=50)
    pixiv_spider.register_callback('add_to_database', 'text', add_to_database, run_in_process=True, no_wrapper=True)
    p = mp.Process(target=url_adder, args=(pixiv_spider.url_queue_for_mp, low, high), name='url_adder')
    p.start()
    #for i in range(10000):
    #   pixiv_spider.add_url('https://app-api.pixiv.net/v1/illust/detail?illust_id=' + str(i), 'add_to_database')
    pixiv_spider.run()