from aiocrawler.spider import RedisSpider, SQLiteSpider
from aiocrawler.helpers import follow_pattern
from bs4 import BeautifulSoup
import os
import sys

def construct_thread_url(s):
	return 'http://tieba.baidu.com' + s

def parse_tab_good(item, queue, url_queue):
	response = item['response'].replace('<!--', '').replace('-->', '')
	follow_pattern(response, r'/p/.*', url_queue, 'parse_thread', construct_thread_url, item['options'])
	#with open('log.html', 'w') as f:
	#	f.write(response)
	'''
	bs = BeautifulSoup(response, 'lxml')
	urls = [i['href'] for i in bs.select('a.j_th_tit')]
	for url in urls:
		url_queue.put({'url': host + url, 'handler': 'parse_thread', 'options': {}})
	'''

def parse_thread(item, queue, url_queue):
	bs = BeautifulSoup(item['response'], 'lxml')
	imgs = bs.find_all(class_='BDE_Image')
	thread_id = item['options']['url'].split('/')[-1]
	img_ids = [img['src'].split('/')[-1] for img in imgs]
	img_path = os.path.join(os.getcwd(), 'img2')
	for pic_id in img_ids:
		if os.path.exists(os.path.join(img_path, pic_id)):
			print('exist!')
			continue
		a = {
			'url': 'http://imgsrc.baidu.com/forum/pic/item/' + pic_id,
			'handler': 'parse_img',
			'options': {}
		}
		url_queue.put(a)

def parse_img(item, queue, url_queue):
	if not os.path.exists('img2'):
		os.makedirs('img2')
	with open('img2/' + item['options']['url'].split('/')[-1], 'wb') as f:
		f.write(item['response'])

if __name__ == '__main__':
	if '--no-proxy' in sys.argv:
		config = {}
	else:
		config={'proxy': 'http://127.0.0.1:1080'}
	if len(sys.argv) >= 3:
		args = []
		for i in sys.argv:
			try:
				arg = int(i)
				args.append(arg)
			except:
				continue
		
		baidu_spider = SQLiteSpider(config={'proxy': 'http://127.0.0.1:1080'})
		baidu_spider.register_callback('parse_tab_good', 'text', parse_tab_good, True)
		baidu_spider.register_callback('parse_thread', 'text', parse_thread, True)
		baidu_spider.register_callback('parse_img', 'binary', parse_img, True)
		for i in range(args[0], args[1]):
			baidu_spider.add_url('http://tieba.baidu.com/f?kw=%E4%B9%83%E6%9C%A8%E5%9D%8246&ie=utf-8&tab=good&cid=&pn=' + str(i * 50), 'parse_tab_good')
		baidu_spider.run()
		