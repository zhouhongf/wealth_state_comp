from config import WealthOutline
from constants import BankDict
import re
from myspiders.ruia import JsonField, RegexField, Item, Spider, Bs4HtmlField, Bs4AttrField, Bs4TextField
from config import Target
from utils.time_util import daytime_standard


label_transfer = {
    '产品名称': 'name',
    '产品代码': 'code',
    '币种': 'currency',
    '风险等级': 'risk',
    '起点金额': 'amount_buy_min',
    '递增金额': 'amount_per_buy',
    '认购起日': 'date_open',
    '认购止日': 'date_close',
    '投资期限': 'term',
    '适用客户群': 'clients',
    '最新年化收益率': 'rate_max',
    '参考年化收益率': 'rate_max',
    '年化收益率': 'rate_max',
    '业绩比较基准': 'rate_max',
    '销售地区': 'sale_areas',
    '发行机构': 'sale_agents',
}


class BankcommItem(Item):
    pattern_rate = re.compile(r'([0-9]+\.?[0-9]*)[%％]*')
    pattern_amount = re.compile(r'[0-9]+\.?[0-9]*')

    bank_name = JsonField(default='交通银行')
    referencable = JsonField(default='较高')

    name = JsonField(json_select='name')
    code = JsonField(json_select='code')
    currency = JsonField(json_select='currency')
    risk = JsonField(json_select='risk')
    amount_buy_min = JsonField(json_select='amount_buy_min')
    amount_per_buy = JsonField(json_select='amount_per_buy')
    date_open = JsonField(json_select='date_open')
    date_close = JsonField(json_select='date_close')
    term = JsonField(json_select='term')
    rate_type = JsonField(json_select='rate_type')
    rate_min = JsonField(json_select='rate_min')
    sale_areas = JsonField(json_select='sale_areas')

    manual_url = JsonField(json_select='manual_url')

    async def clean_risk(self, value):
        if not value:
            return 0
        res = re.compile(r'(\d)R').search(value)
        if not res:
            return 0
        num = res.group(1)
        return int(num)

    async def clean_amount_buy_min(self, value):
        if not value:
            return 0
        res = self.pattern_amount.search(value)
        if not res:
            return 0
        num = float(res.group())
        if '万' in value:
            num = num * 10000
        return num

    async def clean_amount_per_buy(self, value):
        if not value:
            return 0
        res = self.pattern_amount.search(value)
        if not res:
            return 0
        num = float(res.group())
        if '万' in value:
            num = num * 10000
        return num

    async def clean_date_open(self, value):
        if not value:
            return ''
        date = daytime_standard(value)
        return date

    async def clean_date_close(self, value):
        if not value:
            return ''
        date = daytime_standard(value)
        return date

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


    @staticmethod
    def filter_every_row(data_dict: dict):
        data = {
            'name': '',
            'currency': '',
            'risk': '',
            'amount_buy_min': '',
            'amount_per_buy': '',
            'date_open': '',
            'date_close': '',
            'term': '',
            'rate_type': '',
            'rate_min': '',
            'sale_areas': ''
        }
        for k, v in data_dict.items():
            if '产品名称' in k:
                data['name'] = v
            elif '币种' in k:
                data['currency'] = v
            elif '风险' in k:
                data['risk'] = v
            elif '起点金额' in k:
                data['amount_buy_min'] = v
            elif '递增金额' in k:
                data['amount_per_buy'] = v
            elif '认购起日' in k:
                data['date_open'] = v
            elif '认购止日' in k:
                data['date_close'] = v
            elif '投资期限' in k:
                data['term'] = v
            elif '收益率' in k:
                data['rate_type'] = '预期收益型'
                data['rate_min'] = v
            elif '业绩' in k:
                data['rate_type'] = '净值型'
                data['rate_min'] = v
            elif '销售地区' in k:
                data['sale_areas'] = v
        return data


class BankcommLinkItem(Item):
    manual_url_prefix = 'http://www.bankcomm.com/BankCommSite/upload/wmbooks/%s.pdf'
    code = Bs4AttrField(name='li', target='id', attrs={'id': True})


class BankcommWorker(Spider):
    name = 'BankcommWorker'
    bank_name = '交通银行'
    start_urls = ['http://www.bankcomm.com/BankCommSite/jyjr/cn/lcpd/queryFundInfoListNew.do']
    info_url_prefix = 'http://www.bankcomm.com/BankCommSite/jyjr/cn/lcpd/queryFundInfoNew.do?code='

    async def parse(self, response):
        html = await response.text()
        items = await BankcommLinkItem.get_bs4_item(html=html)
        data = items.results
        for one in data['code']:
            code = one.strip()
            manual_url = BankcommLinkItem.manual_url_prefix % code
            url = self.info_url_prefix + code
            outline = {'code': code, 'manual_url': manual_url}
            target = Target(bank_name=self.bank_name, url=url, metadata={'data': outline})
            await self.redis.insert(field=target.id, value=target.do_dump())


def start():
    BankcommWorker.start()

