from myspiders.ruia import JsonField, HtmlField, AttrField, TextField, Item, Spider, Bs4TextField
import re
from config import WealthOutline
from datetime import datetime


class BocItem(Item):
    # 通过使用日期正则表达式，来分割出内容
    pattern_date_in_title = re.compile(r'(.+)(20[0-9]{2}年[01]?[0-9]月[0-3]?[0-9]日)(.+)')
    pattern_code = re.compile(r'[-a-zA-Z0-9]{6,}')
    pattern_code_in_title = re.compile(r'([-a-zA-Z0-9]+)[\u4E00-\u9FA5\s]+')
    pattern_term_in_title = re.compile(r'([0-9]+)天')
    pattern_rate_in_title = re.compile(r'([0-9]+\.?[0-9]*)[%％]')

    target_item = HtmlField(css_select='.main .news ul.list li')

    bank_name = JsonField(default='中国银行')
    referencable = JsonField(default='较高')

    code = TextField(css_select='a')
    manual_url = AttrField(css_select='a', attr='href')

    async def clean_code(self, value):
        self.results['date_open'] = ''
        self.results['name'] = ''
        self.results['term'] = 0
        self.results['rate_min'] = 0.0
        self.results['rate_max'] = 0.0

        one = self.pattern_date_in_title.fullmatch(value)
        if not one:
            two = self.pattern_code.search(value)
            if not two:
                return value
            else:
                code = two.group(0)
                return code
        else:
            code = value

            head = one.group(1)
            tail = one.group(3)

            date_open = one.group(2)
            self.results['date_open'] = datetime.strptime(date_open, "%Y年%m月%d日").strftime('%Y-%m-%d') + ' 00:00:00'

            two = self.pattern_code_in_title.search(head)
            if two:
                code = two.group(1)
                name = head.replace(code, '')
                self.results['name'] = name

            three = self.pattern_term_in_title.search(tail)
            if three:
                self.results['term'] = int(three.group(1))

            four = self.pattern_rate_in_title.findall(tail)
            if four:
                rate_min = float(four[0])
                rate_max = float(four[-1])
                if '%' in value or '％' in value:
                    rate_min = rate_min / 100
                    rate_max = rate_max / 100
                self.results['rate_max'] = round(rate_min, 6)
                self.results['rate_min'] = round(rate_max, 6)
            return code


class BocItemPageCount(Item):
    pattern = re.compile(r'共(\d+)页')
    page_count = Bs4TextField(css_select='.turn_page p', many=False)

    async def clean_page_count(self, value):
        page_count = None
        back = self.pattern.search(value)
        if back:
            page_count = int(back.group(1))
        return page_count


# 总共有50页，每次爬取前5页
class BocWorker(Spider):
    name = 'BocWorker'
    bank_name = '中国银行'
    start_urls = [
        'https://www.boc.cn/fimarkets/cs8/fd6/index.html',
        'https://www.boc.cn/fimarkets/cs8/fd6/index_1.html',
        'https://www.boc.cn/fimarkets/cs8/fd6/index_2.html',
        'https://www.boc.cn/fimarkets/cs8/fd6/index_3.html',
        'https://www.boc.cn/fimarkets/cs8/fd6/index_4.html',
    ]

    async def parse(self, response):
        yield self.extract_boc(response)

    async def extract_boc(self, response):
        html = await response.text()
        async for item in BocItem.get_items(html=html):
            data = item.results
            outline = WealthOutline.do_load(data)
            await self.save_wealth_outline(outline)


def start():
    BocWorker.start()






