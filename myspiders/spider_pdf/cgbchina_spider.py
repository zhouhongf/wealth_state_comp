from myspiders.ruia import JsonField, Item, Spider, Bs4HtmlField, Bs4TextField
from urllib.parse import urlencode, urlparse, urljoin, quote
from config import WealthOutline
import re
from bs4 import BeautifulSoup
from utils.time_util import chinese_to_number, daytime_standard


class CgbchinaWorker(Spider):
    name = 'CgbchinaWorker'
    bank_name = '广发银行'
    bank_domain = 'http://www.cgbchina.com.cn/'
    start_urls = ['http://www.cgbchina.com.cn/Channel/16684283?nav=1', 'http://www.cgbchina.com.cn/Channel/16684283?nav=2']

    manual_url_prefix = 'http://www.cgbchina.com.cn/jsp/pdf/0-%s.pdf'

    pattern_term = re.compile(r'([0-9一二三四五六七八九十]+)个?([天日月年])')
    pattern_rate = re.compile(r'([0-9]+\.?[0-9]*)[%％]*')
    pattern_date = re.compile(r'20[0-9]{2}\-[01]?[0-9]\-[0-3]?[0-9]')

    async def parse(self, response):
        text = await response.text()
        soup = BeautifulSoup(text, 'lxml')
        table = soup.find(id='product_tab')
        list_tr = table.find_all(name='tr')

        for i in range(1, len(list_tr)):
            one = list_tr[i]
            list_td = one.find_all(name='td')
            data = {
                'referencable': '较高',
                'bank_name': self.bank_name,
                'name': list_td[0].text.strip(),
                'code': '',
                'manual_url': '',
                'currency': list_td[1].text.strip(),
                'term': 0,
                'amount_buy_min': int(float(list_td[3].text.strip())),
                'rate_type': '',
                'rate_min': '',
                'rate_max': '',
                'risk': '',
                'date_start': '',
                'date_end': '',
            }

            url = list_td[0].find(name='a').get('href')
            params = urlparse(url).query.split('&')
            for one in params:
                if one.startswith('pno='):
                    code = one.split('=')[1]
                    data['code'] = code
                    data['manual_url'] = self.manual_url_prefix % code
                    break

            term = list_td[2].text.strip()
            if term == '长期':
                data['term'] = 0
            else:
                res_term = self.pattern_term.search(term)
                if res_term:
                    term_num = res_term.group(1)
                    term_unit = res_term.group(2)
                    term_num = chinese_to_number(term_num)
                    if term_unit == '年':
                        data['term'] = round(term_num * 365, 6)
                    elif term_unit == '月':
                        data['term'] = round(term_num * 30, 6)
                    else:
                        data['term'] = term_num

            rate = list_td[4].text.strip()
            if rate:
                if '业绩' in rate:
                    data['rate_type'] = '净值型'
                elif '收益' in rate:
                    data['rate_type'] = '预期收益型'
                list_rate = self.pattern_rate.findall(rate)
                if list_rate:
                    if '%' in rate or '％' in rate:
                        data['rate_min'] = round(float(list_rate[0]) / 100, 6)
                        data['rate_max'] = round(float(list_rate[-1]) / 100, 6)
                    else:
                        data['rate_min'] = float(list_rate[0])
                        data['rate_max'] = float(list_rate[-1])

            risk = list_td[5].text.strip()
            risk_num = re.compile(r'\d').search(risk)
            if risk_num:
                data['risk'] = int(risk_num.group())

            date = list_td[6].text.strip()
            list_date = self.pattern_date.findall(date)
            if list_date:
                data_open = list_date[0]
                data_close = list_date[-1]
                data['date_start'] = daytime_standard(data_open)
                data['date_end'] = daytime_standard(data_close)

            if data['code']:
                outline = WealthOutline.do_load(data)
                await self.save_wealth_outline(outline)



def get_fetch_params(page_index: int):
    form_data = {
        'proName': '',
        'proCode': '',
        'curPage': str(page_index)
    }
    return form_data


class CgbchinaFullPageNumItem(Item):
    pattern = re.compile(r'第1/(\d+)页')
    page_count = Bs4TextField(attrs={'class': 'pages'}, many=False)

    async def clean_page_count(self, value):
        page_count = None
        back = self.pattern.search(value)
        if back:
            page_count = int(back.group(1))
        return page_count


class CgbchinaItemFull(Item):
    manual_url_prefix = 'http://www.cgbchina.com.cn/jsp/pdf/0-%s.pdf'

    target_item = Bs4HtmlField(name='tr', attrs={'class': re.compile(r'single|double')})

    bank_name = JsonField(default='广发银行')
    referencable = JsonField(default='较高')

    name = Bs4TextField(css_select='td:nth-of-type(2)', many=False)
    code = Bs4TextField(css_select='td:nth-of-type(3)', many=False)
    risk = Bs4TextField(css_select='td:nth-of-type(6)', many=False)

    sale_clients = Bs4TextField(css_select='td:nth-of-type(7)', many=False)
    amount_buy_min = Bs4TextField(css_select='td:nth-of-type(8)', many=False)
    term = Bs4TextField(css_select='td:last-of-type', many=False)

    async def clean_code(self, value):
        if value:
            self.results['manual_url'] = self.manual_url_prefix % value
        return value

    async def clean_risk(self, value):
        risk = None
        res = re.compile(r'PR(\d)').search(value)
        if res:
            risk = int(res.group(1))
        return risk

    async def clean_amount_buy_min(self, value):
        res = re.compile(r'\d+\.?\d*').search(value)
        if not res:
            return None
        num = float(res.group(0))
        if '万' in value:
            num = num * 10000
        return num

    async def clean_term(self, value):
        if not value:
            return 0
        res = re.compile(r'\d+').search(value)
        if not res:
            return 0
        num = int(res.group(0))
        if '年' in value:
            num = num * 365
        elif '月' in value:
            num = num * 30
        return num


# 4000 多页一下子集中请求会导致封IP
class CgbchinaWorkerFull(Spider):
    name = 'CgbchinaWorkerFull'
    bank_name = '广发银行'
    bank_domain = 'http://www.cgbchina.com.cn/'
    start_urls = ['http://www.cgbchina.com.cn/Channel/22598110']
    form_data = [get_fetch_params(1)]

    # request_config = {"DELAY": 5, "RETRY_DELAY": 10, "TIMEOUT": 10}

    async def parse(self, response):
        html = await response.text()
        async for item in CgbchinaItemFull.get_bs4_items(html=html):
            yield item

        item_page = await CgbchinaFullPageNumItem.get_bs4_item(html=html)
        page_count = item_page.results['page_count']
        if page_count and page_count > 1:
            print('============  page_count是：%s =================' % page_count)
            list_form_data = [get_fetch_params(index) for index in range(2, page_count + 1)]
            async for resp in self.multiple_request(urls=self.start_urls, form_datas=list_form_data):
                yield self.parse_next(resp)

    async def parse_next(self, response):
        html = await response.text()
        async for item in CgbchinaItemFull.get_bs4_items(html=html):
            yield item

    async def process_item(self, item: Item):
        data = item.results
        outline = WealthOutline.do_load(data)
        print(outline)
        await self.save_wealth_outline(outline)


def start():
    CgbchinaWorker.start()
    # CgbchinaWorkerFull.start()

