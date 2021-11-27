from myspiders.ruia import JsonField, Item, Spider, Bs4TextField, Bs4AttrField, Bs4AttrTextField, Bs4HtmlField
from urllib.parse import urlparse, urljoin, urlencode
from urllib.parse import urlencode
import re
import math
from config import Target
from constants import BankDict
from utils.time_util import daytime_standard


class AbchinaItem(Item):
    pattern_rate = re.compile(r'([0-9]+\.?[0-9]*)[%％]*')
    pattern_amount = re.compile(r'[0-9]+\.?[0-9]*')
    next_url_prefix = 'http://ewealth.abchina.com/fs/intro_list/%s.htm'
    pattern_manual_url = re.compile(r"(url|URL)=\'(.+)\'")

    target_item = JsonField(json_select='Data>Table')

    bank_name = JsonField(default='农业银行')
    referencable = JsonField(default='较高')

    code = JsonField(json_select='ProductNo')
    name = JsonField(json_select='ProdName')

    amount_buy_min = JsonField(json_select='PurStarAmo')
    term = JsonField(json_select='ProdLimit')
    rate_min = JsonField(json_select='ProdProfit')

    redeem_type = JsonField(json_select='ProdClass')
    promise_type = JsonField(json_select='ProdYildType')

    sale_areas = JsonField(json_select='ProdArea')

    date_open = JsonField(json_select='ProdSaleDate')

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

    async def clean_rate_min(self, value):
        if not value:
            self.results['rate_max'] = 0.0
            return 0.0

        if '业绩' in value:
            self.results['rate_type'] = '净值型'
        elif '收益' in value:
            self.results['rate_type'] = '预期收益型'

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

    async def clean_redeem_type(self, value):
        return '封闭式' if '封闭' in value else '开放式'

    async def clean_promise_type(self, value):
        if '浮动' in value:
            self.results['fixed_type'] = '浮动收益'
        elif '固定' in value:
            self.results['fixed_type'] = '固定收益'
        else:
            self.results['fixed_type'] = ''

        if '非保本' in value:
            return '非保本'
        elif '保本' in value:
            return '保本'
        else:
            return ''

    async def clean_sale_areas(self, value):
        if '全国' in value:
            return '全国'
        else:
            return value

    async def clean_date_open(self, value):
        list_date = value.split('-')
        if len(list_date) == 2:
            date_open = list_date[0]
            date_close = list_date[1]
            date_open = daytime_standard(date_open)
            date_close = daytime_standard(date_close)
            self.results['date_close'] = date_close
            return date_open
        else:
            return ''


class AbchinaExtraItem(Item):
    list_risk = BankDict.list_risk

    currency = Bs4TextField(css_select='.con962 .con930 table.list_cp tr:nth-of-type(4) td:last-of-type', many=False)
    date_start = Bs4TextField(css_select='.con962 .con930 table.list_cp tr:nth-of-type(8) td:first-of-type', many=False)
    date_end = Bs4TextField(css_select='.con962 .con930 table.list_cp tr:nth-of-type(8) td:last-of-type', many=False)
    risk = Bs4TextField(css_select='.con962 .con930 table.list_cp tr:nth-of-type(14) td:first-of-type', many=False)

    async def clean_date_start(self, value):
        if not value:
            return ''
        date = daytime_standard(value)
        return date

    async def clean_date_end(self, value):
        if not value:
            return ''
        date = daytime_standard(value)
        return date

    async def clean_risk(self, value):
        if not value:
            return 0
        if value in self.list_risk.keys():
            return self.list_risk[value]
        else:
            return 0


# 日常爬取时，将w设置为“可售|||||||1||0||0”, 即只查询当前在售的理财产品即可，用不着全部爬取
# 如要全部爬取，则将w设置为“|||||||1||0||0”
def get_fetch_url(start_url_prefix: str, page_index: int, page_size: int):
    params = {'i': page_index, 's': page_size, 'o': 0, 'w': '可售|||||||1||0||0'}
    start_url = start_url_prefix + urlencode(params)
    return start_url


class AbchinaWorker(Spider):
    name = 'AbchinaWorker'
    bank_name = '农业银行'
    start_url_prefix = 'http://ewealth.abchina.com/app/data/api/DataService/BoeProductOwnV2?'
    page_size = 15
    start_urls = [get_fetch_url(start_url_prefix, 1, page_size)]

    async def parse(self, response):
        yield self.extract_abchina(response)

        jsondata = await response.json()
        page_total = jsondata['Data']['Table1'][0]['total']
        if page_total and int(page_total) > self.page_size:
            page_count = math.ceil(int(page_total) / self.page_size)
            for index in range(2, page_count + 1):
                url = get_fetch_url(self.start_url_prefix, index, self.page_size)
                target = Target(bank_name=self.bank_name, url=url)
                await self.redis.insert(field=target.id, value=target.do_dump())        # 此处只能用await，不能使用yield，否则会出现pending to another loop的错误提示

    async def extract_abchina(self, response):
        jsondata = await response.json()
        async for item in AbchinaItem.get_json(jsondata=jsondata):
            data = item.results
            code = data['code']
            result = self.collection_outline.find_one({'_id': self.bank_name + '=' + code})
            if not result:
                url = AbchinaItem.next_url_prefix % str(code)
                target = Target(bank_name=self.bank_name, url=url, callback='extract_abchina_next', metadata={'data': data})
                await self.redis.insert(field=target.id, value=target.do_dump())


def start():
    AbchinaWorker.start()






