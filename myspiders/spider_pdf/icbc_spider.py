from myspiders.ruia import Item, Spider, Bs4HtmlField, Bs4AttrField, Bs4TextField, JsonField
from urllib.parse import urlencode, urlparse, urljoin, quote
from config import WealthOutline, Target
import re
from datetime import datetime


class IcbcItem(Item):
    manual_url_prefix = 'https://image.mybank.icbc.com.cn/picture/Perfinancingproduct/%s.pdf'
    pattern_code = re.compile(r'open_protocolSubmit\(\'(.+)\',\'(.+)\',\'.*\'\)')
    pattern_date = re.compile(r'20[0-9]{2}\-[01][0-9]\-[0123][0-9]')
    pattern_rate = re.compile(r'([0-9]+\.?[0-9]*)[%％]*')
    pattern_risk = re.compile(r'PR(\d)-风险')

    target_item = Bs4HtmlField(attrs={'id': re.compile(r'circularcontainer_\d+-wrapper')})

    bank_name = JsonField(default='工商银行')
    referencable = JsonField(default='较高')

    code = Bs4AttrField(target='href', name='a', attrs={'href': re.compile(r'javascript:open_protocolSubmit\(.+\)')}, many=False)
    memo = Bs4AttrField(target='class', name='span', attrs={'class': re.compile(r'ebdp-pc4promote-circularcontainer-tip-.+')})
    date_open = Bs4TextField(name='span', string=re.compile(r'募集期|最近购买开放日'), many=False)

    rate_type = Bs4TextField(css_select='table td:first-of-type span.ebdp-pc4promote-doublelabel-text', many=False)
    rate_min = Bs4TextField(css_select='table td:first-of-type .ebdp-pc4promote-doublelabel-content', many=False)

    amount_buy_min = Bs4TextField(css_select='table td:nth-of-type(2) .ebdp-pc4promote-doublelabel-content', many=False)
    term = Bs4TextField(css_select='table td:nth-of-type(3) .ebdp-pc4promote-doublelabel-content', many=False)

    risk = Bs4TextField(css_select='table td:nth-of-type(4) script', many=False)

    async def clean_code(self, code):
        results = self.pattern_code.search(code)
        if results:
            code = results.group(1)
            name = results.group(2)
            self.results['name'] = name
            self.results['manual_url'] = self.manual_url_prefix % code
        else:
            self.results['name'] = ''
            self.results['manual_url'] = ''
        return code

    async def clean_memo(self, values):
        memo = ''
        for one in values:
            name = one[0]
            keyword = name.split('-')[-1]
            if keyword == 'bao':
                self.results['promise_type'] = '保本'
                continue

            if keyword == 'tradition':
                memo = '传统产品'
                continue
            if keyword == 'mixed':
                memo = '混合型'
                continue
            if keyword == 'gain':
                memo = '固定收益类'
                continue
            if keyword == 'stuctural':
                memo = '结构性存款'
                continue
        return memo

    async def clean_date_open(self, value):
        if not value:
            self.results['date_close'] = ''
            return ''
        res = re.compile(r'\d{8}').findall(value)
        if res:
            date_open = res[0]
            date_close = res[-1]
            date_open = datetime.strptime(date_open, "%Y%m%d").strftime('%Y-%m-%d') + ' 00:00:00'
            self.results['date_close'] = datetime.strptime(date_close, "%Y%m%d").strftime('%Y-%m-%d') + ' 00:00:00'
            return date_open

        result = self.pattern_date.findall(value)
        if result:
            date_open = result[0]
            date_close = result[-1]
            date_open = date_open + ' 00:00:00'
            self.results['date_close'] = date_close + ' 00:00:00'
            return date_open
        return ''

    async def clean_rate_type(self, value):
        if not value:
            return ''
        if '净值' in value or '业绩' in value:
            return '净值型'
        elif '收益' in value:
            return '预期收益型'
        else:
            return ''

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

    async def clean_risk(self, value):
        if not value:
            return 0
        res = self.pattern_risk.search(value)
        if not res:
            return 0
        risk = int(res.group(1))
        return risk


class IcbcDataTableItem(Item):
    data_table = Bs4HtmlField(attrs={'id': 'datatableModel'}, many=False)


class IcbcPageNumItem(Item):
    page_num = Bs4TextField(css_select='#pageturn', many=False)

    async def clean_page_num(self, value):
        res = re.compile(r'\d+').findall(value)
        if not res:
            return 5
        page_num = res[-1]
        return int(page_num)


def get_fetch_params_all(page_count: int):
    form_data = [
        {
            'dse_operationName': 'per_FinanceCurProAllInfoElementP3NSOp',
            'pageFlag_turn': '2',
            'nowPageNum_turn': str(index),
            'structCode': '1',
        } for index in range(2, page_count + 1)
    ]
    return form_data


def get_fetch_params_primary(page_count: int):
    form_data = [
        {
            'dse_operationName': 'per_FinanceCurProListP3NSOp',
            'financeQueryCondition': '$$$$$$$2$%s$11|ALL$1' % str(index),
            'useFinanceSolrFlag': '1',
            'orderclick': '0',
            'menuLabel': '11|ALL',
            'pageFlag_turn': '2',
            'nowPageNum_turn': str(index),
            'Area_code': '1100',
            'structCode': '1',
        } for index in range(2, page_count + 1)
    ]
    return form_data


class IcbcWorker(Spider):
    bank_name = '工商银行'
    start_urls = ['https://mybank.icbc.com.cn/servlet/ICBCBaseReqServletNoSession']

    async def parse(self, response):
        yield self.extract_icbc(response)

        html = await response.text()
        item = await IcbcPageNumItem.get_bs4_item(html=html)
        page_count = item.results['page_num']
        print('【======== %s ========= 页数：%s】' % (self.name, page_count))

        if page_count > 1:
            if self.name == 'IcbcWorkerPrimary':
                form_datas = get_fetch_params_primary(page_count)
            else:
                form_datas = get_fetch_params_all(page_count)
            for one in form_datas:
                target = Target(bank_name=self.bank_name, method='POST', url=self.start_urls[0], formdata=one)
                await self.redis.insert(field=target.id, value=target.do_dump())

    async def extract_icbc(self, response):
        html = await response.text()
        data_table_item = await IcbcDataTableItem.get_bs4_item(html=html)
        data_table_html = data_table_item.results['data_table']
        async for item in IcbcItem.get_bs4_items(soup=data_table_html):
            data = item.results
            outline = WealthOutline.do_load(data)
            await self.save_wealth_outline(outline)


class IcbcWorkerPrimary(IcbcWorker):
    name = 'IcbcWorkerPrimary'
    form_data = [
        {
            'dse_operationName': 'per_FinanceCurProListP3NSOp',
            'financeQueryCondition': '$$$$$$$1$1$11|ALL$1',
            'useFinanceSolrFlag': '1',
            'orderclick': '0',
            'menuLabel': '11|ALL',
            'pageFlag_turn': '1',
            'nowPageNum_turn': '1',
            'Area_code': '1100',
            'structCode': '1',
        }
    ]


class IcbcWorkerAll(IcbcWorker):
    name = 'IcbcWorkerAll'
    form_data = [
        {
            'dse_operationName': 'per_FinanceCurProAllInfoElementP3NSOp',
            'pageFlag_turn': '0',
            'nowPageNum_turn': '1',
            'structCode': '1',
        }
    ]


def start():
    IcbcWorkerPrimary.start()
    # IcbcWorkerAll.start()

