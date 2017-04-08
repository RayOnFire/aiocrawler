from bs4 import BeautifulSoup
import re

def follow_pattern(html, pattern, url_queue, handler, url_constructor=None, options={}):
	bs = BeautifulSoup(html, 'lxml')
	a_tags = bs.select('a')
	for a in a_tags:
		href = a.get('href', None)
		if href:
			if re.match(pattern, href):
				if url_constructor:
					href = url_constructor(href)
				url_queue.put({'url': href, 'handler': handler, 'options': options})