import random
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import datetime
import json
from copy import deepcopy
from spider import Spider
import multiprocessing as mp

folder = 'data'
blog_prefix = 'http://blog.nogizaka46.com/'

def get_members_url(f='members.json'):
    with open(f, 'r') as f:
        j = json.loads(f.read())
    return [(i['urlPrefix'], blog_prefix + i['urlPrefix']) for i in j]

def parse_date(response, spider, options):
    bs = BeautifulSoup(response, 'html.parser')
    dates = [i.text.replace('年', '').replace('月', '') for i in bs.find_all('option')[1:]]
    for d in dates:
        if options:
            o = deepcopy(options)
        else:
            o = {}
        o.update({'page': 1, 'url_pattern': options['url'] + '/?d=' + d + '&p=', 'url': options['url'] + '/?p=1&d=' + d, 'category': d})
        spider.add_url(options['url'] + '/?p=1&d=' + d, 'parse_blog', o)


def parse_blog(queue, url_queue):
    while True:
        item = queue.get()
        #print('parse_blog get item')
        response = item['response']
        #url_queue = item['url_queue']
        options = item['options']
        bs = BeautifulSoup(response, 'html.parser')
        contents = bs.find_all(class_='entrybody')
        converted = []
        imgs = []
        dates = []
        objs = []
        titles = []

        # parse total page
        if options['page'] == 1:
            if bs.find(class_='paginate'):
                total_page = bs.find(class_='paginate').text.replace('\xa0', '')[-5]
                for i in range(2, int(total_page) + 1):
                    o = deepcopy(options)
                    o.update({'url': options['url'] + str(i), 'page': i})
                    url_queue.put({'url': options['url_pattern'] + str(i), 'handler': 'parse_blog', 'options': o})

        # parse dates
        for (index, date) in enumerate(bs.find_all(class_='entrybottom')):
            year, month, day = date.text[:10].split('/')
            hour, minute = date.text[11:16].split(':')
            dates.append(datetime.datetime(int(year), int(month), int(day), int(hour), int(minute)).strftime('%Y-%m-%d-%H-%M-%S'))

        # parse titles
        for title in bs.find_all(class_='entrytitle'):
            titles.append(title.find('a').text)

        # parse contents and construct json
        for (index, blog) in enumerate(contents):
            converted.append([])
            imgs.append([])
            count = 0
            for i in blog:
                if i.find('img') and i.find('a') and type(i.find('a')) != int:
                    try:
                        img_holder_url = i.find('a')['href']
                    except:
                        print(i.find('a'))
                    converted[index].append('{' + str(count) + '}')
                    imgs[index].append((img_holder_url, i.find('img')['src']))
                    count += 1
                else:
                    converted[index].append(str(i))
            objs.append({'member_name': options['member'], 'date': dates[index], 'title': titles[index], 'html': ''.join(converted[index]), 'img_count': count})

        for o in objs:
            with open(folder + '/' + options['member'] + '-' + o['date'] + '.json', 'wb') as f:
                f.write(json.dumps(o).encode('utf8'))
        
        for (blog_index, img_list) in enumerate(imgs):
            for (image_index, img) in enumerate(img_list):
                o = deepcopy(options)
                o.update({'date': objs[blog_index]['date'], 'image_index': image_index, 'refer_from_blog': options['url'], 'refer_blog_index': blog_index, 'thumbnail': img[1]})
                url_queue.put({'url': img[0], 'handler': 'parse_image', 'options': o})

def parse_image(response, spider, options):
    bs = BeautifulSoup(response, 'html.parser')
    origin_img = bs.find('img')['src']
    if not 'expired' in origin_img:
        spider.add_url(origin_img, 'parse_binary', options)
    else:
        #print('image expired')
        spider.add_url(options['thumbnail'], 'parse_binary', options)

def parse_binary(response, spider, options):
    with open(folder + '/' + options['member'] + '-' + options['date'] + '-' + str(options['image_index']) + '.jpg', 'wb') as f:
        f.write(response)
    print('binary write with', options['date'] + '-' + str(options['image_index']) + '.jpg')

def parse_tieba(response, spider, options):
    bs = BeautifulSoup(response.replace('<!--', '').replace('-->', ''), 'html.parser')
    titles = bs.find_all(class_='j_th_tit')


if __name__ == '__main__':
    blog_spider = Spider()
    blog_spider.register_callback('parse_date', 'text', parse_date)
    # send queue to spider
    blog_spider.register_callback('parse_blog', 'text', parse_blog, True)
    blog_spider.register_callback('parse_image', 'text', parse_image)
    blog_spider.register_callback('parse_binary', 'binary', parse_binary)
    blog_spider.add_url(blog_prefix + 'manatsu.akimoto', 'parse_date', {'member': 'manatsu.akimoto'})
    blog_spider.run()

#print(get_members_urlprefix())