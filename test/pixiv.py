from aiocrawler.spider import BaseSpider
import sqlite3
import multiprocessing as mp
import signal

h_pixiv = {
    'App-OS': 'ios',
    'App-OS-Version': '10.3.1',
    'App-Version': '6.7.1',
    'User-Agent': 'PixivIOSApp/6.7.1 (iOS 10.3.1; iPhone8,1)'
}

def init_db():
	conn = sqlite3.connect('F:\\db\\pixiv.db')
	conn.execute(("CREATE TABLE IF NOT EXISTS pixiv ("
					"id INTEGER PRIMARY KEY AUTOINCREMENT,"
					"url TEXT NOT NULL UNIQUE,"
					"info TEXT NOT NULL)"))
	conn.commit()
	conn.close()

def add_to_database(queue, url_queue):
	def on_terminate():
		conn.commit()
		print('committed!')
	signal.signal(signal.SIGTERM, on_terminate)
	conn = sqlite3.connect('F:\\db\\pixiv.db')
	while True:
		item = queue.get()
		#print(item)
		if item == 'TERMINATE':
			conn.commit()
			print("TERMINATE")
			return
		else:
			try:
				conn.execute("INSERT INTO pixiv VALUES (?, ?, ?)", (None, item['options']['url'], item['response']))
			except:
				pass


def url_adder(url_queue):
	try:
		for i in range(0, 10000):
			o = {
				'url': 'https://app-api.pixiv.net/v1/illust/detail?illust_id=' + str(i),
				'handler': 'add_to_database',
				'options': {},
			}
			url_queue.put(o, True, None)
	except KeyboardInterrupt:
		print('url_adder exit!')

#config={'proxy': 'http://127.0.0.1:1080'}

if __name__ == '__main__':
	init_db()
	pixiv_spider = BaseSpider(db_name='F:\\db\\log.db', config={'proxy': 'http://127.0.0.1:1080'}, headers=h_pixiv, sem=20)
	pixiv_spider.register_callback('add_to_database', 'text', add_to_database, run_in_process=True, no_wrapper=True)
	p = mp.Process(target=url_adder, args=(pixiv_spider.url_queue_for_mp,), name='url_adder')
	p.start()
	#for i in range(10000):
	#	pixiv_spider.add_url('https://app-api.pixiv.net/v1/illust/detail?illust_id=' + str(i), 'add_to_database')
	pixiv_spider.run()