from myspiders.ruia import JsonField, Item, Spider, Bs4HtmlField, Bs4AttrField, Bs4TextField
from urllib.parse import urlencode, urlparse, urljoin, quote
import re
from constants import BankDict
from config import WealthOutline, Target



class HfbankItem(Item):
    manual_url_prefix = 'https://mbsn.hfbank.com.cn/ebank/finace_desc?downloadFile=%s_1.htm'
    list_risk = BankDict.list_risk

    pattern_rate = re.compile(r'([0-9]+\.?[0-9]*)[%％]*')
    pattern_date = re.compile(r'[0-9]{2,4}\-[0-9]{1,2}\-[0-9]{1,2}')

    target_item = Bs4HtmlField(name='div', attrs={'class': 'financialist-article'})

    bank_name = JsonField(default='恒丰银行')
    referencable = JsonField(default='较高')

    code = Bs4TextField(css_select='h3 a span', many=False)
    name = Bs4TextField(css_select='h3 a', many=False)

    risk = Bs4TextField(css_select='h3 div span', many=False)
    memo = Bs4TextField(css_select='h3 div', many=False)

    rate_type = Bs4TextField(css_select='table.con tr td', many=False)

    date_open = Bs4TextField(css_select='div.date p:nth-of-type(2)', many=False)
    date_close = Bs4TextField(css_select='div.date p:last-of-type', many=False)
    date_start = Bs4TextField(css_select='div.date p:first-of-type', many=False)
    date_end = Bs4TextField(css_select='div.date p:nth-of-type(3)', many=False)

    rate_min = Bs4TextField(css_select='table.con tr:last-of-type td:first-of-type span', many=False)

    term = Bs4TextField(css_select='table.con tr:last-of-type td:nth-of-type(2)', many=False)
    amount_buy_min = Bs4TextField(css_select='table.con tr:last-of-type td.bot:last-of-type', many=False)

    async def clean_code(self, value):
        self.results['manual_url'] = self.manual_url_prefix % value
        return value

    async def clean_risk(self, value):
        if not value:
            return 0
        if not value in self.list_risk.keys():
            return 0
        return self.list_risk[value]

    async def clean_memo(self, value):
        self.results['promise_type'] = '非保本' if '非保本' in value else '保本'
        self.results['fixed_type'] = '浮动收益' if '浮动' in value else '固定收益'
        return value

    async def clean_rate_type(self, value):
        return '净值型' if '净值' in value else '预期收益型'

    async def clean_date_start(self, value):
        if not value:
            return ''
        if '起息日' not in value:
            return ''
        res = self.pattern_date.search(value)
        if not res:
            return ''
        date = res.group()
        return date + ' 00:00:00'

    async def clean_date_end(self, value):
        if not value:
            return ''
        if '到期日' not in value:
            return ''
        res = self.pattern_date.search(value)
        if not res:
            return ''
        date = res.group()
        return date + ' 00:00:00'

    async def clean_date_open(self, value):
        if not value:
            return ''
        if '起始日' not in value:
            return ''
        res = self.pattern_date.search(value)
        if not res:
            return ''
        date = res.group()
        return date + ' 00:00:00'

    async def clean_date_close(self, value):
        if not value:
            return ''
        if '终止日' not in value:
            return ''
        res = self.pattern_date.search(value)
        if not res:
            return ''
        date = res.group()
        return date + ' 00:00:00'

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


class HfbankItemPageNum(Item):
    page_count = Bs4AttrField(target='value', attrs={'id': 'totalNum'}, many=False)


def get_fetch_formdata(page_index: int, page_size: int):
    form_data = {
        'ptType': 'lcpt',
        'order': 'false',
        'nameValue': 'RsgStrtDt',
        'search': '',
        'TypeNo': '0,1,2',
        'Status': '',
        'Limit': '',
        'RiskLevel': '',
        'CurrType': '',
        'pageStartCount': str(page_index * page_size),
        'pagecount': str(page_size)
    }
    return form_data

# 爬取前3页
class HfbankWorker(Spider):
    name = 'HfbankWorker'
    bank_name = '恒丰银行'
    start_urls = ['http://www.hfbank.com.cn/ucms/hfyh/jsp/gryw/lc_lb.jsp']
    page_size = 8
    form_data = [get_fetch_formdata(one, 8) for one in range(3)]

    headers = {'Referer': 'http://www.hfbank.com.cn/gryw/cfgl/lc/rmlctj/index.shtml'}

    async def parse(self, response):
        html = await response.text()
        async for item in HfbankItem.get_bs4_items(html=html):
            data = item.results
            code = data['code']
            name = data['name']
            data['name'] = name.replace(code, '')
            target = Target(bank_name=self.bank_name, url=data['manual_url'], metadata={'data': data})
            await self.redis.insert(field=target.id, value=target.do_dump())


def start():
    HfbankWorker.start()

