from myspiders.ruia import Spider
from config import WealthOutline
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin


class CzbankWorker(Spider):
    name = 'CzbankWorker'
    bank_name = '浙商银行'
    start_urls = ['http://www.czbank.com/cn/fin_kno/xxcxpt1/lccpxxcx1/']

    async def parse(self, response):
        text = await response.text()
        if text:
            soup = BeautifulSoup(text, 'lxml')
            target = soup.find(title=re.compile(r'在售产品.+'))
            link = target.get('href')
            if not link.startswith('http'):
                url_old = response.url
                url_full = urljoin(url_old, link)
                print('提取出来的url是：', url_full)
                yield self.request(url=url_full, callback=self.parse_table)

    async def parse_table(self, response):
        text = await response.text()
        url_old = response.url
        soup = BeautifulSoup(text, 'lxml')
        list_pdf = soup.find_all(href=re.compile(r'\.pdf'))
        for one in list_pdf:
            code = one.text.strip()
            url = one.get('href')
            if not url.startswith('http'):
                url = urljoin(url_old, url)

            data = {'referencable': '高', 'bank_name': self.bank_name, 'code': code, 'manual_url': url}
            outline = WealthOutline.do_load(data)
            await self.save_wealth_outline(outline)


def start():
    # CzbankWorker.start()
    pass
