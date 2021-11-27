from myspiders.ruia import JsonField, Item, Spider, Bs4HtmlField, Bs4AttrField, Bs4TextField
from urllib.parse import urlencode, urlparse, urljoin, quote
import re
from config import WealthOutline
from utils.time_util import daytime_standard


class HxbItem(Item):
    pattern_rate = re.compile(r'([0-9]+\.?[0-9]*)[%％]*')
    pattern_date = re.compile(r'[0-9]{2,4}\.[0-9]{1,2}\.[0-9]{1,2}')

    target_item = Bs4HtmlField(name='li', attrs={'name': 'pageli'})

    bank_name = JsonField(default='华夏银行')
    referencable = JsonField(default='较高')

    manual_url = Bs4AttrField(target='href', attrs={'href': re.compile(r'\.pdf')}, url_prefix='http://www.hxb.com.cn/', many=False)
    name = Bs4TextField(css_select='p.box_title', many=False)
    rate_type = Bs4TextField(css_select='div.box_lf p:nth-of-type(2)', many=False)

    rate_min = Bs4TextField(css_select='p.box_num', many=False)
    term = Bs4TextField(css_select='ul li:first-of-type span:last-of-type', many=False)

    date_open = Bs4TextField(css_select='ul li:nth-of-type(2)', many=False)
    amount_buy_min = Bs4TextField(css_select='ul li:nth-of-type(3)', many=False)

    sale_ways = Bs4TextField(css_select='ul li:nth-of-type(4) span.riqi', many=False)


    async def clean_rate_type(self, value):
        return '净值型' if '净值' in value else '预期收益型'

    async def clean_rate_min(self, value):
        if not value:
            self.results['rate_max'] = 0.0
            return 0.0
        res = self.pattern_rate.findall(value)
        if not res:
            self.results['rate_max'] = 0.0
            return 0.0
        else:
            rate_min = float(res[0])
            rate_max = float(res[-1])
            if '%' in value or '％' in value:
                rate_min = round(rate_min / 100, 6)
                rate_max = round(rate_max / 100, 6)
            else:
                rate_min = round(rate_min, 6)
                rate_max = round(rate_max, 6)
            self.results['rate_max'] = rate_max
            return rate_min

    async def clean_term(self, value):
        if not value:
            return 0
        res = re.compile(r'\d+').search(value)
        if not res:
            return 0
        num = int(res.group())
        if '年' in value:
            num = num * 365
        elif '月' in value:
            num = num * 30
        return num

    async def clean_date_open(self, value):
        if not value:
            self.results['date_close'] = ''
            return ''
        list_date = self.pattern_date.findall(value)
        if not list_date:
            self.results['date_close'] = ''
            return ''
        date_open = daytime_standard(list_date[0])
        date_close = daytime_standard(list_date[-1])
        self.results['date_close'] = date_close
        return date_open

    async def clean_amount_buy_min(self, value):
        if not value:
            return 0
        res = re.compile(r'\d+\.?\d*').search(value)
        if not res:
            return 0
        num = float(res.group())
        if '万' in value:
            num = num * 10000
        return int(num)

    async def clean_sale_ways(self, value):
        return re.sub(r'\s+', ',', value)


class HxbWorker(Spider):
    name = 'HxbWorker'
    bank_name = '华夏银行'
    start_urls = ['http://www.hxb.com.cn/grjr/lylc/zzfsdlccpxx/index.shtml']
    headers = {'Referer': 'http://www.hxb.com.cn/index.shtml'}

    async def parse(self, response):
        html = await response.text()
        async for item in HxbItem.get_bs4_items(html=html):
            data = item.results
            data['code'] = data['name']
            data['memo'] = '缺少code'
            outline = WealthOutline.do_load(data)
            await self.save_wealth_outline(outline)


def start():
    HxbWorker.start()

