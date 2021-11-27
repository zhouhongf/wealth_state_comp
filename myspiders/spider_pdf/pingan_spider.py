from myspiders.ruia import JsonField, Item, Spider
from config import Target
import math
from copy import copy
import re


class PinganItem(Item):
    start_url_next_prefix = 'https://bank.pingan.com.cn/rmb/bron/ibank/cust/bron-ibank-pd/fund/bootpage/queryPrdAttachedFile.do?prdCode=%s&prdType=01&access_source=PC'
    next_referer_prefix = 'https://bank.pingan.com.cn/m/aum/common/prdInformation/index.html?initPage=true&prdCode=%s&prdType=01'

    currency_transfer = {
        'RMB': '人民币', 156: '人民币',
        'HKD': '港币', 344: '港币',
        'USD': '美元', 840: '美元',
        'GBP': '英镑', 826: '英镑',
        'EUR': '欧元', 978: '欧元',
        'JPY': '日元', 392: '日元',
        'CAD': '加拿大元', 124: '加拿大元',
        'CHF': '瑞士法郎', 756: '瑞士法郎',
        'AUD': '澳大利亚元', '036': '澳大利亚元',
        'SGD': '新加坡元', 702: '新加坡元'
    }

    rate_transfer = {'FF': '非保本浮动收益', 'BG': '保本固定收益', 'BF': '保本浮动收益'}

    target_item = JsonField(json_select='data>list')

    bank_name = JsonField(default='平安银行')
    referencable = JsonField(default='较高')

    code = JsonField(json_select='prdCode')

    name = JsonField(json_select='prdName')
    risk = JsonField(json_select='riskLevel')
    amount_buy_min = JsonField(json_select='minAmount')
    memo = JsonField(json_select='rateType')

    async def clean_risk(self, value):
        if not value:
            return 0
        return int(value)

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

    async def clean_memo(self, value):
        if not value:
            return ''
        if value not in self.rate_transfer.keys():
            return ''
        self.results['promise_type'] = '非保本' if value == 'FF' else '保本'
        self.results['fixed_type'] = '固定收益' if value == 'BG' else '浮动收益'
        return self.rate_transfer[value]


def get_fetch_params(page_index: int, page_size: int):
    form_data = {
        'tableIndex': 'table01',
        'dataType': '01',
        'tplId': 'tpl01',
        'pageNum': str(page_index),
        'pageSize': str(page_size),
        'channelCode': 'C0002',
        'access_source': 'PC',
    }
    return form_data


class PinganWorker(Spider):
    name = 'PinganWorker'
    bank_name = '平安银行'
    page_size = 20
    start_urls = ['https://ebank.pingan.com.cn/rmb/brop/pop/cust/brop_pop_shelf_service.qrySuperviseProductList']
    form_data = [get_fetch_params(1, page_size)]
    headers = {'Referer': 'https://ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true'}

    async def parse(self, response):
        jsondata = await response.json()
        async for item in PinganItem.get_json(jsondata=jsondata):
            data = item.results
            code = data['code']
            result = self.collection_outline.find_one({'_id': self.bank_name + '=' + code})
            if not result:
                url = PinganItem.start_url_next_prefix % data['code']
                referer = PinganItem.next_referer_prefix % data['code']
                headers = copy(self.headers)
                headers.update({'Referer': referer})
                target = Target(bank_name=self.bank_name, url=url, headers=headers, callback='extract_pingan_next', metadata={'list_one': data})
                await self.redis.insert(field=target.id, value=target.do_dump())

        total_size = jsondata['data']['totalSize']
        page_count = math.ceil(int(total_size) / self.page_size)
        print('==== %s ===== 总共%s条记录，共%s页 =======' % (self.bank_name, total_size, page_count))
        if page_count > 1:
            for page_index in range(2, page_count + 1):
                form_data = get_fetch_params(page_index, self.page_size)
                target = Target(bank_name=self.bank_name, method='POST', url=self.start_urls[0], headers=self.headers, formdata=form_data)
                await self.redis.insert(field=target.id, value=target.do_dump())


def start():
    PinganWorker.start()

