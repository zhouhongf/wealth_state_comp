from utils.string_util import random_digits
from constants import BankDict
from config import WealthOutline, Target
import time
import os
import re
from myspiders.ruia import JsonField, HtmlField, AttrField, TextField, RegexField, Item, Spider, Bs4HtmlField, Bs4AttrField, Bs4TextField
from datetime import datetime


branch_list = [
    {'branchId': '703220', 'city': '南京'},
    {'branchId': '703260', 'city': '合肥'},
    {'branchId': '703400', 'city': '福州'},
    {'branchId': '711020', 'city': '卡中心'},
    {'branchId': '711100', 'city': '北京'},
    {'branchId': '721100', 'city': '大连'},
    {'branchId': '722100', 'city': '沈阳'},
    {'branchId': '723000', 'city': '天津'},
    {'branchId': '724100', 'city': '石家庄'},
    {'branchId': '725100', 'city': '西安'},
    {'branchId': '726100', 'city': '太原'},
    {'branchId': '727100', 'city': '呼和浩特'},
    {'branchId': '728100', 'city': '南昌'},
    {'branchId': '729100', 'city': '南宁'},
    {'branchId': '730100', 'city': '昆明'},
    {'branchId': '731109', 'city': '上海'},
    {'branchId': '732300', 'city': '苏州'},
    {'branchId': '733600', 'city': '宁波'},
    {'branchId': '733990', 'city': '杭州'},
    {'branchId': '734200', 'city': '厦门'},
    {'branchId': '737001', 'city': '青岛'},
    {'branchId': '737200', 'city': '济南'},
    {'branchId': '738100', 'city': '武汉'},
    {'branchId': '739109', 'city': '郑州'},
    {'branchId': '740110', 'city': '长沙'},
    {'branchId': '741100', 'city': '成都'},
    {'branchId': '742109', 'city': '重庆'},
    {'branchId': '744000', 'city': '广州'},
    {'branchId': '744100', 'city': '深圳'},
    {'branchId': '744809', 'city': '东莞'},
    {'branchId': '745100', 'city': '哈尔滨'},
    {'branchId': '746100', 'city': '兰州'},
    {'branchId': '747109', 'city': '贵阳'},
    {'branchId': '748100', 'city': '长春'},
    {'branchId': '750100', 'city': '乌鲁木齐'},
    {'branchId': '754000', 'city': '海口'},
    {'branchId': '758000', 'city': '银川'},
    {'branchId': '759000', 'city': '西宁'},
    {'branchId': '768100', 'city': '拉萨'}
]


def get_fetch_params(page_index: int, page_size: int):
    form_data = {
        'currType': '',                     # 币   种， 01：人民币， 02：美元， 03：英镑， 04：欧元， 05：港币， 06：其他
        'dayDeadline': '',                  # 产品期限， 00：无固定期限，01：30天以下，02：31天到90天，03：91天到180天，04：181天到365天，05：365天以上
        'riskLevel': '',                    # 风险等级， 00：无风险， 01：低风险， 02：较低风险， 03：中等风险， 04：较高风险， 05：高风险
        'firstAmt': '',                     # 购买起点， 01：0至5万， 02：5至10万， 03：10至20万， 04：20万以上
        'branchId': '701100',
        'prodState': '',                    # 产品状态， 01：可购买， 02：即将发售  04：不能购买
        'openAttr': '',                     # 开放性质， 01：开放式， 02：封闭式
        'breakEvenAttr': '',                # 保本性质， 01：保本， 02：不保本
        'endInterestDate': '',              # 产品到期日， 譬如20190910，代表2019年9月10日
        'finaQrycondi': '',                 # 根据输入的产品代码或名称进行模糊搜索
        'totuseAmt': '01',                  # 销售额度， 01：显示全部产品， 02：仅显示有剩余额度的产品
        'orderField': 'ipo_start_date',     # 排序标准， ppo_incomerate：预期年化收益率， ipo_start_date：募集期， risk_level：风险等级， prod_name：产品名称， day_prddeadline：产品期限， first_amt：购买起点
        'orderType': 'desc',                # 降序升序， desc:降序排列， asc:升序排列
        'currentPage': str(page_index),
        'pageSize': str(page_size),
        'tcstNo': '',
        'userId': '',
        'pwdControlFlag': '0',
        'responseFormat': 'JSON',
        'random': random_digits(4)
    }
    return form_data


'''
appAmt: null
autoReinvestFlag: "1"
beginIntereastDate: "2020-06-04 00:00:00.0"
branchId: "701100"
buyFlag: "1"
buyUnit: null
channelPrdFlag: "0"
channels: "0137"
clientType: "2"
currType: "014"
dayDeadLine: 365
dxbz: "0"
endIntereastDate: "2021-06-04 23:59:59.0"
extPrdType: "0"
firstAmt: "2000.00"
firstFlag: null
guestMaxRatio: "0"
guestMinRatio: "0"
incomerate: "2.7"
ipoBeginDate: "2019-05-09 00:00:00.0"
ipoEndDate: "2019-05-13 23:59:59.0"
isFocus: "0"
lastIncomeRate: "0.00"
nav: "1.00"
navDate: "2020-05-16"
nextOpenDate: "2020-06-04"
pAppAmt: "0.00"
pAppSAmt: "0.00"
pMaxAmt: "0.00"
pMaxRed: "0.00"
pMaxSAmt: "0.00"
pMinRed: "1000.00"
pSubSUnit: "100.00"
pSubUnit: null
perTotUse: "99.89"
ppdCpfl: "5"    ???
prdAttr: "2"    ???
prdName: "乐赢美元稳健周期365天（钞户专属）"
prdNo: "A192A8232"
prdState: "4"
prdType: "1"    ???
prodBookName: null
redFlag: "0"
redUnit: "0.00"
riskLevel: "较低风险"
startPAmt: null
taCode: "ZX"
taName: "中信银行"
tempFlag: "0"
totUseAmt: "4994688700.00"
totalAmt: "5000000000.00"
transMode: "0"          ???
transStatus: "001 00 0 0000000000000   000000  00000000 0000000000000000001100000000000000000000000000000000000000"
'''


class CiticbankItem(Item):
    list_risk = BankDict.list_risk
    pattern_rate = re.compile(r'([0-9]+\.?[0-9]*)[%％]*')
    pattern_amount = re.compile(r'[0-9]+\.?[0-9]*')
    manual_url_prefix = 'https://etrade.citicbank.com/portalweb/findoc/%s00.html'

    target_item = JsonField(json_select='content>resultList')

    bank_name = JsonField(default='中信银行')
    referencable = JsonField(default='较高')

    code = JsonField(json_select='prdNo')
    name = JsonField(json_select='prdName')
    risk = JsonField(json_select='riskLevel')
    amount_buy_min = JsonField(json_select='firstAmt')

    date_open = JsonField(json_select='ipoBeginDate')
    date_close = JsonField(json_select='ipoEndDate')
    date_start = JsonField(json_select='beginIntereastDate')
    date_end = JsonField(json_select='endIntereastDate')

    currency = JsonField(json_select='currType')
    term = JsonField(json_select='dayDeadLine')

    rate = JsonField(json_select='incomerate')
    rate_min = JsonField(json_select='guestMinRatio')
    rate_max = JsonField(json_select='guestMaxRatio')

    async def clean_code(self, value):
        self.results['manual_url'] = self.manual_url_prefix % value
        return value

    async def clean_risk(self, value):
        if not value:
            return 0
        if not value in self.list_risk.keys():
            return 0
        return self.list_risk[value]

    async def clean_amount_buy_min(self, value):
        if not value:
            return 0
        res = self.pattern_amount.search(value)
        if not res:
            return 0
        num = float(res.group())
        if '万' in value:
            num = num * 10000
        return int(num)

    async def clean_date_open(self, value):
        if not value:
            return ''
        datetime_obj = datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
        return datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

    async def clean_date_close(self, value):
        if not value:
            return ''
        datetime_obj = datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
        return datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

    async def clean_date_start(self, value):
        if not value:
            return ''
        datetime_obj = datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
        return datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

    async def clean_date_end(self, value):
        if not value:
            return ''
        datetime_obj = datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
        return datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

    async def clean_currency(self, value):
        if int(value) == 1:
            return '人民币'
        elif int(value) == 14:
            return '美元'
        else:
            return ''

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

    async def clean_rate(self, value):
        if not value:
            return 0.0
        res = self.pattern_rate.search(value)
        if not res:
            return 0.0
        rate = float(res.group())
        rate = round(rate / 100, 6)
        return rate

    async def clean_rate_min(self, value):
        if not value:
            return 0.0
        res = self.pattern_rate.search(value)
        if not res:
            return 0.0
        rate = float(res.group())
        rate = round(rate / 100, 6)
        return rate

    async def clean_rate_max(self, value):
        if not value:
            return 0.0
        res = self.pattern_rate.search(value)
        if not res:
            return 0.0
        rate = float(res.group())
        rate = round(rate / 100, 6)
        return rate


# 仅爬取前5页即可
class CiticbankWorker(Spider):
    name = 'CiticbankWorker'
    bank_name = '中信银行'
    headers = {'Referer': 'https://etrade.citicbank.com/portalweb/html/finList.html'}
    form_data = [get_fetch_params(one, 10) for one in range(1, 6)]
    start_urls = ["https://etrade.citicbank.com/portalweb/fd/getFinaList.htm"]

    async def parse(self, response):
        jsondata = await response.json(content_type='text/html')
        async for item in CiticbankItem.get_json(jsondata=jsondata):
            data = item.results
            rate = data['rate']
            data.pop('rate')

            rate_min = data['rate_min']
            rate_max = data['rate_max']
            if rate_min == 0.0 and rate != 0.0:
                data['rate_min'] = rate
            if rate_max == 0.0 and rate != 0.0:
                data['rate_max'] = rate

            print(data)
            target = Target(bank_name=self.bank_name, url=data['manual_url'], metadata={'data': data})
            await self.redis.insert(field=target.id, value=target.do_dump())


def start():
    CiticbankWorker.start()

