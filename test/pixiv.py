from aiocrawler.spider import BaseSpider
import sqlite3

h_pixiv = {
    'App-OS': 'ios',
    'App-OS-Version': '10.3.1',
    'App-Version': '6.7.1',
    'User-Agent': 'PixivIOSApp/6.7.1 (iOS 10.3.1; iPhone8,1)'
}

def init_db():
	conn = sqlite3.connect('pixiv.db')
	conn.execute(("CREATE TABLE IF NOT EXISTS pixiv ("
					"id INTEGER PRIMARY KEY AUTOINCREMENT,"
					"url TEXT NOT NULL UNIQUE,"
					"info TEXT NOT NULL)"))
	conn.commit()
	conn.close()

def add_to_database(queue, url_queue):
	conn = sqlite3.connect('pixiv.db')
	while True:
		item = queue.get()
		try:
			conn.execute("INSERT INTO pixiv VALUES (?, ?, ?)", (None, item['options']['url'], item['response']))
		except:
			pass
		conn.commit()

#config={'proxy': 'http://127.0.0.1:1080'}

if __name__ == '__main__':
	init_db()
	pixiv_spider = BaseSpider(headers=h_pixiv, sem=20)
	pixiv_spider.register_callback('add_to_database', 'text', add_to_database, run_in_process=True, no_wrapper=True)
	for i in range(100000):
		pixiv_spider.add_url('https://app-api.pixiv.net/v1/illust/detail?illust_id=' + str(i), 'add_to_database')
	pixiv_spider.run()