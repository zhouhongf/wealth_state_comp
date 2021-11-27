import math
from config import WealthOutline, CONFIG, Target
import os
import re
from myspiders.ruia import JsonField, Item, Spider, Bs4AttrField
from urllib.parse import urlencode, urlparse, urljoin, quote
from datetime import datetime
from utils.time_util import daytime_standard


branch_code_name = [
    {"code": "340", "name": "安徽省"},
    {"code": "110", "name": "北京市"},
    {"code": "500", "name": "重庆市"},
    {"code": "212", "name": "大连市"},
    {"code": "350", "name": "福建省"},
    {"code": "440", "name": "广东省"},
    {"code": "450", "name": "广西省"},
    {"code": "620", "name": "甘肃省"},
    {"code": "520", "name": "贵州省"},
    {"code": "460", "name": "海南省"},
    {"code": "130", "name": "河北省"},
    {"code": "410", "name": "河南省"},
    {"code": "230", "name": "黑龙江"},
    {"code": "420", "name": "湖北省"},
    {"code": "430", "name": "湖南省"},
    {"code": "220", "name": "吉林省"},
    {"code": "320", "name": "江苏省"},
    {"code": "360", "name": "江西省"},
    {"code": "210", "name": "辽宁省"},
    {"code": "150", "name": "内蒙古"},
    {"code": "331", "name": "宁波市"},
    {"code": "640", "name": "宁夏区"},
    {"code": "371", "name": "青岛市"},
    {"code": "630", "name": "青海省"},
    {"code": "370", "name": "山东省"},
    {"code": "140", "name": "山西省"},
    {"code": "610", "name": "陕西省"},
    {"code": "310", "name": "上海市"},
    {"code": "510", "name": "四川省"},
    {"code": "442", "name": "深圳市"},
    {"code": "322", "name": "苏州市"},
    {"code": "120", "name": "天津市"},
    {"code": "351", "name": "厦门市"},
    {"code": "540", "name": "西藏区"},
    {"code": "650", "name": "新疆区"},
    {"code": "530", "name": "云南省"},
    {"code": "330", "name": "浙江省"}
]
#  各省份分行ID
branch_codes = [
    '340', '110', '500', '212', '350',
    '440', '450', '620', '520', '460',
    '130', '410', '230', '420', '430',
    '220', '320', '360', '210', '150',
    '331', '640', '371', '630', '370',
    '140', '610', '310', '510', '442',
    '322', '120', '351', '540', '650',
    '530', '330'
]
# 0 非净值型, 1 净值型
is_net_values = ['0', '1']


'''
allOrgFlag: "1"
brand: "03"
channel: "网点,网银,其它,手机银行,网站"
channel_sig: null
code: "SN072019000007D01"
collBgnDate: 1559134800000
collEndDate: 1559554200000
currencyType: "01"
dcrIndex: "20191118_1574065468"
ext1: null
ext2: null
ext3: null
ext4: null
hotStatus: "0"
hot_stauts: null
instructionUrl: "产品说明书|@@|http://finance.ccb.com/cn/finance/productnews/newsdetail/20190528_259714524.html"
invalidateDate: 2079187200000
investBgnDate: 1559577600000
investEndDate: 4102329600000
investPeriod: 29430
investPeriodEnd: null
investPeriodStart: null
isCcbcomPro: "1"
isNetvalPro: "1"
name: "乾元-恒赢180天大众版周期开放式净值型产品"
netval: null
netvalDate: null
pdType: "8_1"
proMode: "1"
provinceId: null
pubNoticeUrl: "产品公告|@@|"
purFloorAmt: 10000
purStepAmt: 1
reserveBgnTime: null
reserveEgnTime: null
riskLevel: "02"
saleStatus: "02"
validateDate: 1574006400000
yieldRate: 3.67
yieldRateEnd: null
yieldRateStart: null
yieldSpec: "0"
'''


class CcbItem(Item):
    pattern_rate = re.compile(r'([0-9]+\.?[0-9]*)[%％]*')
    pattern_amount = re.compile(r'[0-9]+\.?[0-9]*')
    bank_domain = 'http://www.ccb.com/'
    pattern_http_url = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
    manual_url_ignore = 'http://www.ccb.com/sz/cn/fhgg/fhgg_list_1.html'

    target_item = JsonField(json_select='ProdList')

    bank_name = JsonField(default='建设银行')
    referencable = JsonField(default='较高')

    code = JsonField(json_select='code')
    name = JsonField(json_select='name')

    term = JsonField(json_select='investPeriod')
    risk = JsonField(json_select='riskLevel')
    amount_buy_min = JsonField(json_select='purFloorAmt')
    amount_per_buy = JsonField(json_select='purStepAmt')

    rate_min = JsonField(json_select='yieldRate')

    date_open = JsonField(json_select='collBgnDate')
    date_close = JsonField(json_select='collEndDate')
    date_start = JsonField(json_select='investBgnDate')
    date_end = JsonField(json_select='investEndDate')

    currency = JsonField(json_select='currencyType')

    sale_ways = JsonField(json_select='channel')
    sale_areas = JsonField(json_select='provinceId')

    rate_type = JsonField(json_select='isNetvalPro')
    redeem_type = JsonField(json_select='proMode')
    promise_type = JsonField(json_select='yieldSpec')

    manual_url = JsonField(json_select='instructionUrl')
    notice_issue_url = JsonField(json_select='pubNoticeUrl')

    async def clean_term(self, value):
        if not value:
            return 0
        value = str(value)
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
        res = re.compile(r'\d+').search(str(value))
        if not res:
            return 0
        num = res.group()
        return int(num)

    async def clean_amount_buy_min(self, value):
        if not value:
            return 0
        value = str(value)
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
        value = str(value)
        res = re.compile(r'\d+\.?\d*').search(value)
        if not res:
            return 0
        num = float(res.group())
        if '万' in value:
            num = num * 10000
        return int(num)


    async def clean_rate_min(self, value):
        if not value:
            self.results['rate_max'] = 0.0
            return 0.0

        value = str(value)
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


    async def clean_date_open(self, value):
        if not value:
            return ''
        if re.compile(r'\d{13}').fullmatch(str(value)):
            return datetime.fromtimestamp(value / 1000.0).strftime('%Y-%m-%d') + ' 00:00:00'
        else:
            return ''

    async def clean_date_close(self, value):
        if not value:
            return ''
        if re.compile(r'\d{13}').fullmatch(str(value)):
            return datetime.fromtimestamp(value / 1000.0).strftime('%Y-%m-%d') + ' 00:00:00'
        else:
            return ''

    async def clean_date_start(self, value):
        if not value:
            return ''
        if re.compile(r'\d{13}').fullmatch(str(value)):
            return datetime.fromtimestamp(value / 1000.0).strftime('%Y-%m-%d') + ' 00:00:00'
        else:
            return ''

    async def clean_date_end(self, value):
        if not value:
            return ''
        if re.compile(r'\d{13}').fullmatch(str(value)):
            return datetime.fromtimestamp(value / 1000.0).strftime('%Y-%m-%d') + ' 00:00:00'
        else:
            return ''

    async def clean_currency(self, value):
        if not value:
            return ''

        if value == '01':
            return '人民币'
        elif value == "12":
            return '英镑'
        elif value == '13':
            return '港币'
        elif value == '14':
            return '美元'
        elif value == '15':
            return '瑞士法郎'
        elif value == '27':
            return '日元'
        elif value == '28':
            return '加元'
        elif value == '29':
            return '澳元'
        elif value == '33':
            return '欧元'
        else:
            return ''

    async def clean_sale_areas(self, value):
        if not value:
            return '全国'
        else:
            value_back = ''
            if isinstance(value, str):
                value_list = value.split(',')
                for code in value_list:
                    for one in branch_code_name:
                        if code == one['code']:
                            value_back += one['name']
            return value_back

    async def clean_rate_type(self, value):
        if not value:
            return ''

        if value == '0':
            return '预期收益型'
        elif value == '1':
            return '净值型'
        else:
            return ''

    async def clean_redeem_type(self, value):
        if not value:
            return ''

        if value == '0':
            return '封闭式'
        elif value == '1':
            return '开放式'
        else:
            return ''

    async def clean_promise_type(self, value):
        if not value:
            return ''

        if value == '0':
            return '非保本'
        elif value == '1':
            return '保本'
        else:
            return ''

    async def clean_manual_url(self, value):
        if not value:
            return ''
        res = self.pattern_http_url.search(value)
        if not res:
            return ''
        manual_url = res.group(0)
        if manual_url == self.manual_url_ignore:
            return ''
        suffix = os.path.splitext(manual_url)[-1]
        if suffix in CONFIG.FILE_SUFFIX and not manual_url.startswith('http'):
            manual_url = urljoin(self.bank_domain, manual_url)
        return manual_url

    async def clean_notice_issue_url(self, value):
        if not value:
            return ''
        value_need = value.replace('产品公告|@@|', '')
        if not value_need.startswith('http'):
            value_need = urljoin(self.bank_domain, value_need)
        return value_need


# queryForm.isNetvalPro, 1 净值型, 0 非净值型
# queryForm.provinceId，省份ID前3位
# queryForm.saleStatus, 销售状态 -1 在售 03 可预约 04 待售 05 已售完， 如不添加，则包含全部状态的理财产品
# queryForm.brand，  03 乾元 01 利得盈 02 建行财富 04 汇得盈 05 其他
# queryForm.yieldSpec: 0 非保本 1 保本
def get_fetch_params(page_index: int, province_id: str, is_net_value: str):
    formdata = {
        'queryForm.isNetvalPro': is_net_value,
        'queryForm.provinceId': province_id,
        'queryForm.saleStatus': -1,
        'pageNo': page_index,
        'pageSize': 12
    }
    return formdata


class CcbWorker(Spider):
    name = 'CcbWorker'
    bank_name = '建设银行'
    headers = {'Referer': 'http://finance.ccb.com/cn/finance/product.html'}
    begin_url = 'http://finance.ccb.com/cc_webtran/queryFinanceProdList.gsp'

    # 根据各省份ID和是否是净值型，先获取第一页内容，得到各种类型下的总页数多少
    async def start_manual(self):
        for one in branch_codes:
            for two in is_net_values:
                formdata = get_fetch_params(1, one, two)
                yield self.request(url=self.begin_url, formdata=formdata, callback=self.parse, metadata={'branch_code': one, 'is_net_value': two})

    async def parse(self, response):
        yield self.extract_ccb(response)

        jsondata = await response.json(content_type='text/html')
        # 在获取第一页，得到各种情况下的总页数后，再进一步获取剩余的页数内容
        if response.metadata:
            branch_code = response.metadata['branch_code']
            is_net_value = response.metadata['is_net_value']

            total_count = jsondata['totalCount']
            if total_count:
                page_count = math.ceil(total_count / 12)
                if page_count > 1:
                    for index in range(2, page_count + 1):
                        formdata = get_fetch_params(index, branch_code, is_net_value)
                        target = Target(bank_name=self.bank_name, method='POST', url=self.begin_url, formdata=formdata, headers=self.headers)
                        await self.redis.insert(field=target.id, value=target.do_dump())

    async def extract_ccb(self, response):
        jsondata = await response.json(content_type='text/html')
        async for item in CcbItem.get_json(jsondata=jsondata):
            data = item.results
            manual_url = data['manual_url']
            if manual_url:
                suffix = os.path.splitext(manual_url)[-1]
                if suffix in CONFIG.FILE_SUFFIX:
                    outline = WealthOutline.do_load(item.results)
                    await self.save_wealth_outline(outline)
                else:
                    target = Target(bank_name=self.bank_name, url=manual_url, callback='extract_ccb_next', metadata={'data': data})
                    await self.redis.insert(field=target.id, value=target.do_dump())
            else:
                outline = WealthOutline.do_load(item.results)
                await self.save_wealth_outline(outline)


def start():
    CcbWorker.start()

