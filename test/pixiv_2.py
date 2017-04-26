from aiocrawler.spider import BaseSpider
import sqlite3
import json
import os
import sys
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

def get_json():
    with open('rem2.json', 'r') as f:
        li = json.loads(f.read())
    return li

if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] == 'proxy':
        config = {'proxy': 'http://127.0.0.1:1080'}
    else:
        config = {}
    pixiv_spider = BaseSpider(config=config, sem=100, headers={'referer': 'https://app-api.pixiv.net/'})
    pixiv_spider.register_callback('parse_img', 'binary', parse_img, no_wrapper=False)
    for u in get_json():
        if u:
            pixiv_spider.add_url(u, 'parse_img')
    pixiv_spider.run()