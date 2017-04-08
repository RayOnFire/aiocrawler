import requests
from bs4 import BeautifulSoup
from pprint import pprint

headers = {
	'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
}

def get(url, headers=None):
	return requests.get(url, headers=headers)

def _b(t):
	return BeautifulSoup(t, 'lxml')