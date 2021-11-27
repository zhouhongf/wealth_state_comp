from myspiders.ruia import JsonField, Item, Spider
import json
from config import WealthOutline, Target
from myspiders.tools.tools_spider import fetch_bank_cookies
from urllib.parse import quote, urljoin
import re

# 产品类型 参数：
# 固定期限：   (product_type=3)*finance_limittime = %*(finance_currency = 01)*(finance_state='可购买')
# 现金管理类： (product_type=4)*finance_limittime = %*(finance_currency = 01)*(finance_state='可购买')
# 净值类：     (product_type=2)*finance_limittime = %*(finance_currency = 01)*(finance_state='可购买')
# 汇理财：                      finance_limittime = %*(finance_currency = 01)*(finance_state='可购买')
# 私行专属：   (product_type=0)*finance_limittime = %*(finance_currency = 01)*(finance_state='可购买')
# 专属产品：   (product_type=1)*finance_limittime = %*(finance_currency = 01)*(finance_state='可购买')
# 销售状态 参数：
# 即将发售：  (product_type=3)*finance_limittime = %*(finance_currency = 01)*(finance_state='即将发售')
# 可购买：    (product_type=3)*finance_limittime = %*(finance_currency = 01)*(finance_state='可购买')  默认
# 不可购买：  (product_type=3)*finance_limittime = %*(finance_currency = 01)*(finance_state='不可购买')
# 已过期：    (product_type=3)*finance_limittime = %*(finance_currency = 01)*(finance_state='已过期')

# 下载PDF文件 也需要cookies


class SpdbItem(Item):
    pattern_rate = re.compile(r'([0-9]+\.?[0-9]*)[%％]*')
    risk_transfer = {'0': 0, 'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5}

    bank_name = JsonField(default='浦发银行')
    referencable = JsonField(default='较高')

    code = JsonField(json_select='finance_no')
    name = JsonField(json_select='finance_allname')
    risk = JsonField(json_select='finance_risklevel')
    amount_buy_min = JsonField(json_select='finance_indi_ipominamnt')
    term = JsonField(json_select='finance_limittime')
    rate_min = JsonField(json_select='finance_anticipate_rate')
    manual_url = JsonField(json_select='product_attr')
    notice_url = JsonField(json_select='docpuburl')

    async def clean_risk(self, value):
        if not value:
            return 0
        if value not in self.risk_transfer.keys():
            return 0
        return self.risk_transfer[value]

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

        res = self.pattern_rate.findall(value)
        if not res:
            self.results['rate_max'] = 0.0
            return 0.0
        else:
            rate_min = float(res[0])
            rate_max = float(res[-1])
            rate_min = round(rate_min / 100, 6)
            rate_max = round(rate_max / 100, 6)
            self.results['rate_max'] = rate_max
            return rate_min

    async def clean_manual_url(self, value):
        if not value:
            return ''
        if not value.startswith('http'):
            value = urljoin('https://per.spdb.com.cn/', value)
        return value

    async def clean_notice_url(self, value):
        if not value:
            return ''
        if not value.startswith('http'):
            value = urljoin('https://per.spdb.com.cn/', value)
        return value


def get_form_data_one(page_index, searchword):
    formdata = {
        "metadata": "finance_state|finance_no|finance_allname|finance_anticipate_rate|finance_limittime|finance_lmttime_info|finance_type|docpuburl|finance_ipo_enddate|finance_indi_ipominamnt|finance_indi_applminamnt|finance_risklevel|product_attr|finance_ipoapp_flag|finance_next_openday",
        "channelid": "266906",
        "page": str(page_index),
        "searchword": searchword,
    }
    return formdata


# 仅供爬取 汇理财 使用
def get_form_data_two(page_index, searchword):
    formdata = {
        "metadata": "finance_allname|finance_anticipate_rate|finance_limittime|finance_indi_ipominamnt|finance_type|finance_no|finance_state|docpuburl|finance_risklevel|product_attr",
        "channelid": "263468",
        "page": str(page_index),
        "searchword": searchword,
    }
    return formdata


def get_words_from_formdata(form_data):
    currency = ''
    searchword = form_data['searchword']
    pattern_currency = re.compile(r'finance_currency\s*=\s*(\d+)')
    result = pattern_currency.search(searchword)
    if result:
        num = result.group(1)
        if num:
            if num == '01':
                currency = '人民币'
            elif num == '12':
                currency = '英镑'
            elif num == '13':
                currency = '港币'
            elif num == '14':
                currency = '美元'

    rate_type = '预期收益型'
    pattern_rate_type = re.compile(r'product_type\s*=\s*(\d+)')
    res = pattern_rate_type.search(searchword)
    if res:
        num = res.group(1)
        if num == '2':
            rate_type = '净值型'
    return currency, rate_type


class SpdbWorker(Spider):
    name = 'SpdbWorker'
    bank_name = '浦发银行'
    bank_domain = 'https://per.spdb.com.cn/'
    headers = {'Referer': 'https://per.spdb.com.cn/bank_financing/financial_product'}
    begin_url = 'https://per.spdb.com.cn/was5/web/search'

    # finance_state_list = ['可购买', '即将发售', '不可购买', '已过期']
    currency_dicts = {'01': '人民币', '14': '美元', '12': '英镑', '13': '港币'}
    product_type_dicts = {'0': '私行专属', '1': '专属产品', '2': '净值类', '3': '固定期限', '4': '现金管理类'}
    # product_type_special = '汇理财'

    async def start_manual(self):
        cookie_need = await fetch_bank_cookies(self.bank_name)
        self.headers.update({'Cookie': cookie_need})

        list_formdata = []
        for product_type in self.product_type_dicts.keys():
            for currency in self.currency_dicts.keys():
                searchword = "(product_type=%s)*finance_limittime = %s*(finance_currency = %s)*(finance_state='可购买')" % (product_type, '%', currency)
                form_data = get_form_data_one(1, searchword)
                list_formdata.append(form_data)
        # 爬取汇理财
        for currency in self.currency_dicts.keys():
            searchword = "finance_limittime = %s*(finance_currency = %s)*(finance_state='可购买')" % ('%', currency)
            form_data = get_form_data_two(1, searchword)
            list_formdata.append(form_data)

        for form_data in list_formdata:
            yield self.request(url=self.begin_url, formdata=form_data, callback=self.parse, metadata={'form_data': form_data})

    async def parse(self, response):
        form_data = response.metadata['form_data']
        text = await response.text()
        text = text.strip()
        print(text)
        if text:
            jsondata = json.loads(text)
            currency, rate_type = get_words_from_formdata(form_data)
            data_list = jsondata['rows']
            async for item in SpdbItem.get_json(jsondata=data_list):
                data = item.results
                data['currency'] = currency
                data['rate_type'] = rate_type
                print(data)
                outline = WealthOutline.do_load(data)
                await self.save_wealth_outline(outline)

            # 如果form_data中page参数是1，则表示是才请求完第一页，因此需要检查是否还有后面的页数，可以爬取
            page_total = jsondata['pageTotal']
            page_index_current = form_data['page']
            if page_index_current == '1':
                if page_total and int(page_total) > 1:
                    for page_index in range(2, int(page_total) + 1):
                        form_data['page'] = str(page_index)
                        target = Target(bank_name=self.bank_name, method='POST', url=self.begin_url, formdata=form_data, headers=self.headers, metadata={'form_data': form_data})
                        await self.redis.insert(field=target.id, value=target.do_dump())


def start():
    SpdbWorker.start()

