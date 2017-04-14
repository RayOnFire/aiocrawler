from aiocrawler.spider import BaseSpider
import sqlite3
import multiprocessing as mp
import signal
import os

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

def add_to_database(queue: mp.Queue, url_queue: mp.Queue) -> None:
	#def on_terminate(sig, frame):
	#	conn.commit()
	#	conn.close()
	#	print('committed!')
	#	os.kill(mp.current_process().pid, signal.SIGTERM)
	#signal.signal(signal.SIGINT, on_terminate)
    def commit(sig, frame):
        conn.commit()
        print('pixiv commit!')
    signal.setitimer(signal.ITIMER_REAL, 5, 5)
    signal.signal(signal.SIGALRM, commit)
	conn = sqlite3.connect('pixiv3.db')
	while True:
		item = queue.get()
		try:
			conn.execute("INSERT INTO pixiv VALUES (?, ?, ?)", (None, item['options']['url'], item['response']))
			conn.commit()
		except:
			pass


def url_adder(url_queue: mp.Queue) -> None:
	try:
		for i in range(5000000, 10000000):
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
	init_db()
	pixiv_spider = BaseSpider(db_name='log2.db', headers=h_pixiv, sem=500)
	pixiv_spider.register_callback('add_to_database', 'text', add_to_database, run_in_process=True, no_wrapper=True)
	p = mp.Process(target=url_adder, args=(pixiv_spider.url_queue_for_mp,), name='url_adder')
	p.start()
	#for i in range(10000):
	#	pixiv_spider.add_url('https://app-api.pixiv.net/v1/illust/detail?illust_id=' + str(i), 'add_to_database')
	pixiv_spider.run()