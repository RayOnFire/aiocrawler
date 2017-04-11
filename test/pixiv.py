from aiocrawler.spider import BaseSpider

h_pixiv = {
    'App-OS': 'ios',
    'App-OS-Version': '10.3.1',
    'App-Version': '6.7.1',
    'User-Agent': 'PixivIOSApp/6.7.1 (iOS 10.3.1; iPhone8,1)'
}

config={'proxy': 'http://127.0.0.1:1080'}
if __name__ == '__main__':
	pixiv_spider = BaseSpider(config=config, headers=h_pixiv, sem=20)
	for i in range(1):
		pixiv_spider.add_url('https://app-api.pixiv.net/v1/illust/detail?illust_id=' + str(i))
	pixiv_spider.run()