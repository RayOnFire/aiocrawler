import requests
from bs4 import BeautifulSoup
from pprint import pprint

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
}

h_pixiv = {
    'App-OS': 'ios',
    'App-OS-Version': '10.3.1',
    'App-Version': '6.7.1',
    'User-Agent': 'PixivIOSApp/6.7.1 (iOS 10.3.1; iPhone8,1)'
}

def get(url, headers=None):
    return requests.get(url, headers=headers)

def gp(id):
    params = {
        'illust_id': id,
    }
    return requests.get('https://app-api.pixiv.net/v1/illust/detail', params=params, headers=h_pixiv)

def _b(t):
    return BeautifulSoup(t, 'lxml')