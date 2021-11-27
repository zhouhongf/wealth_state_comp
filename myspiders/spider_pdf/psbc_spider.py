from myspiders.ruia import JsonField, Item, Spider
from urllib.parse import urlencode, urlparse, urljoin, quote
from config import WealthOutline, Target
import time
import math
import re


def get_fetch_url(start_url_prefix: str, page_start_number: int, page_size: int):
    params = {
        'turnPageBeginPos': str(page_start_number),
        'turnPageShowNum': str(page_size),
        'currentBusinessCode': '00681011',
        'responseFormat': 'JSON',
        'channel': '1101',
        'version': 'stanver',
        '_': str(int(time.time() * 1000))
    }
    url = start_url_prefix + urlencode(params)
    return url


def get_fetch_url_substitute(start_url_prefix: str, page_start_number: int, page_size: int):
    params = {
        'turnPageBeginPos': str(page_start_number),
        'turnPageShowNum': str(page_size),
        'productType': '2',
        'responseFormat': 'JSON',
        'channel': '1101',
        'version': 'stanver',
        '_': str(int(time.time() * 1000))
    }
    url = start_url_prefix + urlencode(params)
    return url


class PsbcItem(Item):
    manual_url_prefix = 'http://www.psbc.com/data/tosend/resource/upload/%s.pdf'
    prod_types = {
        '01': '封闭式净值型',
        '02': '封闭式非净值型',
        '03': '开放式净值型',
        '04': '开放式非净值型'
    }

    target_item = JsonField(json_select='iFinancialList')

    bank_name = JsonField(default='邮储银行')
    referencable = JsonField(default='较高')

    code = JsonField(json_select='financeCode')
    name = JsonField(json_select='financeName')
    code_register = JsonField(json_select='financeRegistCode')
    risk = JsonField(json_select='riskLevel')

    async def clean_code(self, value):
        self.results['manual_url'] = self.manual_url_prefix % value
        return value

    async def clean_risk(self, value):
        if not value:
            return 0
        res = re.compile(r'\d').search(value)
        if not res:
            return 0
        num = res.group()
        return int(num)


# 每次爬取前10页
class PsbcWorker(Spider):
    name = 'PsbcWorker'
    bank_name = '邮储银行'
    start_url_prefix = 'https://pbank.psbc.com/perbank/protalFinancialProductQuery.do?'
    headers = {'Referer': 'https://pbank.psbc.com/perbank/financialProductInfoQuery.gate'}
    # start_url_prefix = 'https://pbank.psbc.com/perbank/portalPersonProductManageQuery.do?'
    # headers = {'Referer': 'https://pbank.psbc.com/perbank/portalPersonProductManageQuery.gate'}

    page_size = 10
    start_urls = [get_fetch_url('https://pbank.psbc.com/perbank/protalFinancialProductQuery.do?', one, 10) for one in range(1, 11)]

    async def parse(self, response):
        jsondata = await response.json(content_type='text/html')
        async for item in PsbcItem.get_json(jsondata=jsondata):
            outline = WealthOutline.do_load(item.results)
            await self.save_wealth_outline(outline)

        # total_num = jsondata['turnPageTotalNum']
        # page_count = math.ceil(int(total_num) / self.page_size)
        # for index in range(2, page_count + 1):
        #     url = get_fetch_url(self.start_url_prefix, index, self.page_size)
        #     target = Target(bank_name=self.bank_name, url=url, headers=self.headers)
        #     await self.redis.insert(field=target.id, value=target.do_dump())


def start():
    PsbcWorker.start()

