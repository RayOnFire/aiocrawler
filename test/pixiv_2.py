from aiocrawler.spider import BaseSpider
import sqlite3
import json
import os
from pprint import pprint

def parse_img(item, queue, url_queue):
	if not os.path.exists('img'):
		os.makedirs('img')
	with open('img/' + item['options']['url'].split('/')[-1], 'wb') as f:
		f.write(item['response'])

def get_urls():
	conn = sqlite3.connect('pixiv.db')
	cur = conn.cursor()
	cur.execute("SELECT info FROM pixiv")
	return [json.loads(i[0])['illust']['meta_single_page']['original_image_url'] for i in cur.fetchall()]

if __name__ == '__main__':
	pixiv_spider = BaseSpider(sem=100, headers={'referer': 'https://app-api.pixiv.net/'})
	pixiv_spider.register_callback('parse_img', 'binary', parse_img, True)
	for u in get_urls():
		if not 'limit' in u:
			pixiv_spider.add_url(u, 'parse_img')
	pixiv_spider.run()