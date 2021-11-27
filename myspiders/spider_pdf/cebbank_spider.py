from myspiders.ruia import JsonField, Item, Spider, Bs4HtmlField, Bs4AttrField, Bs4TextField
from urllib.parse import urlencode, urlparse, urljoin, quote
from config import Target
import re
from constants import BankDict


def get_fetch_formdata(page_index: int, rate_type: str):
    one = {
        'codeOrName': '',
        'TZBZMC': 'RMB',
        'sylxArr[]': rate_type,      # 收益类型，00为预期收益， 01为业绩基准， 02为净值型
        'SFZS': 'Y',                 # 是否在售
        'qxrUp': 'Y',
        'qxrDown': '',
        'dqrUp': '',
        'dqrDown': '',
        'qdjeUp': '',
        'qdjeDown': '',
        'qxUp': '',
        'qxDown': '',
        'yqnhsylUp': '',
        'yqnhsylDown': '',
        'page': str(page_index),
        'pageSize': '12'
    }
    one = urlencode(one)

    label = 'channelIds[]'
    label_values = ['yxl94', 'ygelc79', 'hqb30', 'dhb2', 'cjh', 'gylc70', 'Ajh67', 'Ajh84', '901776', 'Bjh91', 'Ejh99', 'Tjh70', 'tcjh96', 'ts43', 'ygjylhzhMOM25', 'yxyg87', 'zcpzjh64',
                    'wjyh1', 'smjjb9', 'ty90', 'tx16', 'ghjx6', 'ygxgt59', 'wbtcjh3', 'wbBjh77', 'wbTjh28', 'sycfxl', 'cfTjh', 'jgdhb', 'tydhb', 'jgxck', 'jgyxl', 'tyyxl', 'dgBTAcp',
                    '27637097', '27637101', '27637105', '27637109', '27637113', '27637117', '27637121', '27637125', '27637129', '27637133', 'gyxj32', 'yghxl', 'ygcxl', 'ygjxl', 'ygbxl',
                    'ygqxl', 'yglxl', 'ygzxl', 'ygttg']
    two = ''
    for value in label_values:
        data = {label: value}
        two += '&' + urlencode(data)

    return '&' + one + two


class CebbankItem(Item):
    list_risk = BankDict.list_risk
    pattern_rate = re.compile(r'([0-9]+\.?[0-9]*)[%％]*')
    pattern_amount = re.compile(r'[0-9]+\.?[0-9]*')
    pattern_term = re.compile(r'([0-9]+)[日月年天]')

    join_url = 'http://www.cebbank.com/site/gryw/yglc/lccp49/index.html'
    pattern_link = re.compile(r'/site/gryw/yglc/lccpsj/.+/index\.html')

    target_item = Bs4HtmlField(css_select='.lccp_main_content_tx ul li')

    bank_name = JsonField(default='光大银行')
    referencable = JsonField(default='较高')
    currency = JsonField(default='人民币')

    code = Bs4AttrField(name='a', target='data-analytics-click', attrs={'href': pattern_link}, many=False)
    name = Bs4AttrField(name='a', target='title', attrs={'href': pattern_link}, many=False)
    url_next = Bs4AttrField(name='a', target='href', attrs={'href': pattern_link}, many=False, url_prefix=join_url)

    risk = Bs4TextField(name='p')

    async def clean_code(self, value):
        list_value = value.split('-')
        code = list_value[-1]
        return code

    async def clean_risk(self, value):
        risk = 0
        list_value = []
        for one in value:
            if one:
                list_value.append(one)
        for one in list_value:
            if one in self.list_risk.keys():
                risk = self.list_risk[one]
            elif '收益率' in one or '净值' in one:
                res = self.pattern_rate.findall(one)
                if not res:
                    self.results['rate_max'] = 0.0
                    self.results['rate_min'] = 0.0
                else:
                    rate_need = []
                    for dig in res:
                        num = float(dig)
                        if 0.0 < num < 10.0:
                            rate_need.append(num)
                    if not rate_need:
                        self.results['rate_max'] = 0.0
                        self.results['rate_min'] = 0.0
                    else:
                        rate_min = rate_need[0]
                        rate_max = rate_need[-1]
                        if '%' in one or '％' in one:
                            rate_min = round(rate_min / 100, 6)
                            rate_max = round(rate_max / 100, 6)
                        else:
                            rate_min = round(rate_min, 6)
                            rate_max = round(rate_max, 6)
                        self.results['rate_max'] = rate_max
                        self.results['rate_min'] = rate_min
            elif '期限' in one:
                res = self.pattern_term.search(one)
                if not res:
                    self.results['term'] = 0
                else:
                    num = int(res.group(1))
                    if '年' in one:
                        num = num * 365
                    elif '月' in one:
                        num = num * 30
                    self.results['term'] = num
            elif '起点金额' in one:
                res = self.pattern_amount.search(one)
                if not res:
                    self.results['amount_buy_min'] = 0
                else:
                    num = float(res.group())
                    if '万' in one:
                        num = num * 10000
                    self.results['amount_buy_min'] = int(num)
        return risk


class CebbankPageItem(Item):
    page_count = Bs4TextField(attrs={'id': 'totalpage'}, many=False)


class CebbankWorker(Spider):
    name = 'CebbankWorker'
    bank_name = '光大银行'
    headers = {'Referer': 'http://www.cebbank.com/site/gryw/yglc/lccp49/index.html'}
    begin_url = 'http://www.cebbank.com/eportal/ui?moduleId=12073&struts.portlet.action=/app/yglcAction!listProduct.action'
    list_rate_type = {'00': '预期收益型', '02': '净值型'}

    async def start_manual(self):
        for k in self.list_rate_type.keys():
            form_data = get_fetch_formdata(1, k)
            url = self.begin_url + form_data
            yield self.request(url=url, method='POST', callback=self.parse, metadata={'rate_type': k})

    async def parse(self, response):
        rate_type = response.metadata['rate_type']
        html = await response.text()
        # 根据获取的url_next，准备爬取产品信息页，获取PDF链接
        async for item in CebbankItem.get_bs4_items(html=html):
            data = item.results
            data['rate_type'] = self.list_rate_type[rate_type]
            target_next = Target(bank_name=self.bank_name, url=data['url_next'], callback='extract_cebbank_next', metadata={'data': data})
            await self.redis.insert(field=target_next.id, value=target_next.do_dump())

        # 根据页码数，准备爬取后面几页的内容
        item_page = await CebbankPageItem.get_bs4_item(html=html)
        page_count = item_page.results['page_count']
        if page_count and int(page_count) > 1:
            for page_index in range(2, int(page_count) + 1):
                form_data = get_fetch_formdata(page_index, rate_type)
                url = self.begin_url + form_data
                rate_type_chinese = self.list_rate_type[rate_type]
                target = Target(bank_name=self.bank_name, method='POST', url=url, metadata={'rate_type': rate_type_chinese})
                await self.redis.insert(field=target.id, value=target.do_dump())


def start():
    CebbankWorker.start()

