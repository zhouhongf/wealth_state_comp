from myspiders.ruia import JsonField, AttrField, Item, Spider
from urllib.parse import urlencode, urlparse
from config import CONFIG, Target
import random
import demjson
import re
import os

'''
AreaCode: ""
BeginDate: "2020-05-15"
CRateType: ""
CapitalProtectName: ""
Currency: "人民币"
EndDate: "2020-05-18"
ExpireDate: "2020-07-20"
FinDate: "63天"
IncresingMoney: "0"
InitMoney: "0"
IsCanBuy: "true"
IsInfinite: "0"
IsNewFlag: "False"
NetValue: ""
PrdCode: "Q01511"
PrdName: "挂钩黄金两层区间二个月结构性存款"
ProxyText: ""
REGCode: ""
RateFlag: "0"
RateHigh: ""
RateLow: ""
Risk: "R1(谨慎型)"
SaleChannel: "1|2|3|4"
SaleChannelName: "网上|手机|PAD|网点"
ShowExpectedReturn: ""
ShowExpireDate: "2020-07-20"
Status: "A"
Style: "浮动收益型"
Term: "31天-90天（含）"
TypeCode: "010011"
'''


class CmbchinaItem(Item):
    pattern_date = re.compile(r'20[0-9]{2}-[01][0-9]-[0123][0-9]')
    pattern_rate = re.compile(r'([0-9]+\.?[0-9]*)[%％]*')

    target_item = JsonField(json_select='list')

    bank_name = JsonField(default='招商银行')
    referencable = JsonField(default='较高')

    code = JsonField(json_select='PrdCode')
    name = JsonField(json_select='PrdName')
    code_register = JsonField(json_select='REGCode')
    risk = JsonField(json_select='Risk')
    amount_buy_min = JsonField(json_select='InitMoney')
    amount_per_buy = JsonField(json_select='IncresingMoney')

    date_open = JsonField(json_select='BeginDate')
    date_close = JsonField(json_select='EndDate')
    date_end = JsonField(json_select='ExpireDate')

    currency = JsonField(json_select='Currency')
    term = JsonField(json_select='FinDate')

    rate_min = JsonField(json_select='RateLow')
    rate_max = JsonField(json_select='RateHigh')

    fixed_type = JsonField(json_select='Style')

    sale_ways = JsonField(json_select='SaleChannelName')
    sale_areas = JsonField(json_select='AreaCode')

    async def clean_code_register(self, value):
        if not value:
            return ''
        return value

    async def clean_risk(self, value):
        res = re.compile(r'R(\d)').search(value)
        if not res:
            return 0
        num = res.group(1)
        return int(num)

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

    async def clean_amount_per_buy(self, value):
        if not value:
            return 0
        res = re.compile(r'\d+\.?\d*').search(value)
        if not res:
            return 0
        num = float(res.group())
        if '万' in value:
            num = num * 10000
        return int(num)

    async def clean_date_open(self, value):
        if not value:
            return ''
        res = self.pattern_date.fullmatch(value)
        if not res:
            return ''
        date = res.group()
        return date + ' 00:00:00'

    async def clean_date_close(self, value):
        if not value:
            return ''
        res = self.pattern_date.fullmatch(value)
        if not res:
            return ''
        date = res.group()
        return date + ' 00:00:00'

    async def clean_date_end(self, value):
        if not value:
            return ''
        res = self.pattern_date.fullmatch(value)
        if not res:
            return ''
        date = res.group()
        return date + ' 00:00:00'


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
            return 0.0
        res = self.pattern_rate.search(value)
        if not res:
            return 0.0
        else:
            rate = float(res.group())
            if '%' in value or '％' in value:
                rate = round(rate / 100, 6)
            else:
                rate = round(rate, 6)
            return rate

    async def clean_rate_max(self, value):
        if not value:
            return 0.0
        res = self.pattern_rate.search(value)
        if not res:
            return 0.0
        else:
            rate = float(res.group())
            if '%' in value or '％' in value:
                rate = round(rate / 100, 6)
            else:
                rate = round(rate, 6)
            return rate


    async def clean_fixed_type(self, value):
        if '浮动' in value:
            return '浮动收益'
        elif '固定' in value:
            return '固定收益'
        else:
            return ''

    async def clean_sale_ways(self, value):
        if not value:
            return ''
        return value.replace('|', ',')


class CmbchinaPDFMobileItem(Item):
    manual_url_prefix_mobile = 'https://mobile.cmbchina.com/IEntrustFinance/FinanceProduct/FP_PrdInstruction.aspx?Code='
    domain_mobile = 'mobile.cmbchina.com'
    url = AttrField(css_select='#ctl00_cphBody_info_PDF', attr='onclick')

    async def clean_url(self, link):
        links = link.split('=')
        url = links[-1].strip()
        url = url[1:-1]
        suffix = os.path.splitext(url)[-1]
        return url if suffix in CONFIG.FILE_SUFFIX else None


class CmbchinaPDFWebItem(Item):
    manual_url_prefix_web = 'http://www.cmbchina.com/cfweb/Personal/productdetail.aspx?code=%s&type=prodexplain'
    url = AttrField(css_select='#content_panel a', attr='href')

    async def clean_url(self, url):
        suffix = os.path.splitext(url)[-1]
        return url if suffix in CONFIG.FILE_SUFFIX else None


def get_fetch_urls(start_url_prefix: str, page_index: int, page_size: int):
    params = {
        'op': 'search',
        'type': 'm',
        'pageindex': str(page_index),
        'salestatus': '',                           # A可购买， B即将发售
        'baoben': '',                               # Y保本， N不保本
        'currency': '',                             # 10人民币，32美元，43英镑，35欧元，65日元，21港币，39加元，87瑞郎，29澳元，69新元
        'term': '',                                 # 1：7天含以下，2：8天-14天（含），3：15天-30天（含），4：31天-90天（含），5：91天-180天（含），6：181天-365天（含），7：365天以上
        'keyword': '',                              # 产品关键词
        'series': '01',                             # 01：不限，010017：私人银行单一资产系列，010012：私人银行家业常青系列，010016：私人银行多元配置系列，010015：私人银行专享联动系列，010013：私行债券套利系列，010011：A009 结构性存款系列，010001：海外寻宝系列，010002：焦点联动系列，010003：日日金系列，010004：新股申购系列，010005：安心回报系列，010006：稳健收益型外币系列，010007：招银进宝系列，010008：A股掘金系列，010009：私人银行系列
        'risk': '',                                 # R1(谨慎型)，R2(稳健型)，R3(平衡型)，R4(进取型)，R5(激进型)
        'city': '',
        'date': '',
        'pagesize': str(page_size),
        'orderby': 'ord1',                          # ord1发售时间（降序），ord2发售时间（升序），ord3收益率（降序），ord4收益率（升序）, ord5风险级别（降序），ord6风险级别（升序）, ord7产品到期日（降序）,ord8产品到期日（升序），ord9理财期限（降序），ord10理财期限（升序）
        't': str(random.random()),
        'citycode': '',                             # 同行政区号，如0512苏州，0025南京
    }
    start_url = start_url_prefix + urlencode(params)
    return start_url

# 每次爬取前5页
class CmbchinaWorker(Spider):
    name = 'CmbchinaWorker'
    bank_name = '招商银行'
    headers = {'Referer': 'http://www.cmbchina.com/cfweb/Personal/Default.aspx'}
    page_size = 20
    start_url_prefix = 'http://www.cmbchina.com/cfweb/svrajax/product.ashx?'
    start_urls = [get_fetch_urls('http://www.cmbchina.com/cfweb/svrajax/product.ashx?', one, 20) for one in range(1, 6)]

    async def parse(self, response):
        content = await response.text()
        jsondata = demjson.decode(content[1:-1])
        async for item in CmbchinaItem.get_json(jsondata=jsondata):
            data = item.results
            code = data['code']
            result = self.collection_outline.find_one({'_id': self.bank_name + '=' + code})
            if not result:
                url_next = CmbchinaPDFMobileItem.manual_url_prefix_mobile + code
                target = Target(bank_name=self.bank_name, url=url_next, metadata={'data': data})
                await self.redis.insert(field=target.id, value=target.do_dump())



def start():
    CmbchinaWorker.start()

