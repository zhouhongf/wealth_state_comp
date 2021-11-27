from config import WealthOutline, Document, Target
from myspiders.ruia import JsonField, Item, Spider
import re
import farmhash
from utils.time_util import chinese_to_number


'''
Benchmark: "0"
BenchmarkType: "0"
BuyType: "2"
CacheUpdateTime: "2020-05-18 12:45:01"
CashFlag: "0"
ChannelsName: "柜台;网银;电话;手机;直销;"
ClearDays: "0"
ClientGroups: "0"
ClientGroupsName: "全部类别"
CloseTime: "15:30:00"
ContractFile: "C1030519002236"
CurrType: "156"
CurrTypeName: "人民币"
CurrentDate: "2020-05-18"
DAvailShare: "576252051.00"
DScheShare: "94800150000.00"
EndDate: "2099-12-31"
FundMode: "2"
FundModeName: "非保本浮动型"
IncomePer10000: 0.7446
IncomeProd: null
IncomeRate: 0.0311
IncomeRate7D: 0.031141
IncomeRateEnd: 0.031141
IncomeRateExt: "3.11%"
IncomeRateStart: 0.031141
IncomeType: "2"
IncomeTypeName: "变动收益"
IpoEndDate: "2038-07-29 23:59:59"
IpoStartDate: "2019-07-29 09:00:00"
IpoState: "1"
IsExclusivePrd: "0"
IsNewCifPrd: "0"
IsWhitePrd: "0"
LinkedPrdName: null
LinkedPrdType: null
LiveTime: "1"
Nav: 1
NextAvailShare: "0"
NextIncomeRate: 0
NextQuotaBeginTime: ""
NextQuotaEndTime: ""
NextQuotaExit: "0"
NextScheShare: "0"
ObserveDate: null
OpenTime: "00:01:00"
PappAmt: 100
PerLimit: 5000000
PfirstAmt: 10000
PmaxAmt: 100000000
PmaxRed: 0
PminHold: 1000
PminRed: 0.01
PrdAttr: "A"
PrdAttrName: "资产类"
PrdAvailShare: "0"
PrdBuyShare: "0"
PrdCode: "FSAF19189A"
PrdFeature: "资金灵活"
PrdImgName: "lingdongbanner.png"
PrdName: "民生天天增利灵动款理财产品"
PrdNextDate: "2020-05-19"
PrdScheShare: "0"
PrdState: "2"
PredUnit: 0.01
ProdType: "5"
ProdTypeName: "活期型"
PsubUnit: 100
QuotaBeginTime: "2020-05-18 00:00:00"
QuotaEndTime: "2020-05-18 23:59:59"
QuotaTimeState: "1"
RealEndDate: "2099-12-31"
RedeemType: "2"
Remarks: null
RenegeInterTypeName: null
RiskFirstFlag: "0"
RiskFlag: "Y"
RiskLevel: "2"
RiskLevelName: "较低风险(二级)"
StartDate: "2020-05-18"
TssPrdName: "民生天天增利灵动款理财产品"
UnitLiveTime: "1天"
'''

class CmbcItem(Item):
    pattern_rate = re.compile(r'([0-9]+\.?[0-9]*)[%％]*')
    pattern_date = re.compile(r'20[0-9]{2}-[01][0-9]-[0123][0-9]')

    bank_name = JsonField(default='民生银行')
    referencable = JsonField(default='较高')

    code = JsonField(json_select='PrdCode')
    code_register = JsonField(json_select='ContractFile')
    name = JsonField(json_select='PrdName')
    risk = JsonField(json_select='RiskLevel')
    currency = JsonField(json_select='CurrTypeName')
    term = JsonField(json_select='LiveTime')

    amount_buy_min = JsonField(json_select='PfirstAmt')
    amount_buy_max = JsonField(json_select='PmaxAmt')
    amount_per_buy = JsonField(json_select='PsubUnit')

    sale_ways = JsonField(json_select='ChannelsName')
    sale_clients = JsonField(json_select='ClientGroupsName')

    rate_min = JsonField(json_select='IncomeRateStart')
    rate_max = JsonField(json_select='IncomeRateEnd')

    date_open = JsonField(json_select='IpoStartDate')
    date_close = JsonField(json_select='IpoEndDate')
    date_start = JsonField(json_select='StartDate')
    date_end = JsonField(json_select='RealEndDate')

    promise_type = JsonField(json_select='FundModeName')
    redeem_type = JsonField(json_select='ProdTypeName')

    async def clean_risk(self, value):
        return int(value) if value else 0

    async def clean_term(self, value):
        return int(value) if value else 0

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

    async def clean_amount_buy_max(self, value):
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

    async def clean_sale_ways(self, value):
        return value.replace(';', ',') if value else ''

    async def clean_rate_min(self, value):
        if not value:
            return 0.0
        value = str(value)
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
        value = str(value)
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

    async def clean_date_open(self, value):
        if not value:
            return ''
        res = self.pattern_date.search(value)
        if not res:
            return ''
        date = res.group(0)
        return date + ' 00:00:00'

    async def clean_date_close(self, value):
        if not value:
            return ''
        res = self.pattern_date.search(value)
        if not res:
            return ''
        date = res.group(0)
        return date + ' 00:00:00'

    async def clean_date_start(self, value):
        if not value:
            return ''
        res = self.pattern_date.search(value)
        if not res:
            return ''
        date = res.group(0)
        return date + ' 00:00:00'

    async def clean_date_end(self, value):
        if not value:
            return ''
        res = self.pattern_date.search(value)
        if not res:
            return ''
        date = res.group(0)
        return date + ' 00:00:00'

    async def clean_promise_type(self, value):
        if not value:
            return ''
        if '浮动' in value:
            self.results['fixed_type'] = '浮动收益'
        elif '固定' in value:
            self.results['fixed_type'] = '固定收益'
        else:
            self.results['fixed_type'] = ''

        if '非保本' in value:
            self.results['rate_type'] = '净值型'
            return '非保本'
        elif '保本' in value:
            self.results['rate_type'] = '预期收益型'
            return '保本'
        else:
            return ''

    async def clean_redeem_type(self, value):
        if not value:
            return ''
        if '开放' in value or '活期' in value:
            return '开放式'
        elif '封闭' in value or '定期' in value:
            return '封闭式'
        else:
            return ''


'''
CASH_DAY: "16"
CLOSE_TIME: "15:30:00"
CRFLAGNAME: "现钞                                                      "
CURR_TYPE_NAME: "人民币                     "
EDDATE: ""
FORCE_MODE: " "
INCOME_RATE: "3.05%"
INTEREST_TYPE_NAME: "ACT/365             "
IPO_END_DATE: "20140317"
IPO_START_DATE: "20140317"
LIV_TIME_UNIT_NAME: "1天      "
NAV: "1.0000"
NEXT_END_DATE: "        "
Next_Income_Rate: "3.05%"
OPDATE: ""
OPEN_TIME: "05:00:00"
PDAY_MAX: "0.00"
PFIRST_AMT: "10,000.00"
PMAX_AMT: "100,000,000.00"
PMAX_RED: "10,000,000.00"
PMIN_HOLD: "10,000.00"
PRD_ATTR_NAME: "资产类                     "
PRD_CODE: "FSAC14168A  "
PRD_NAME: "非凡资产管理天溢金普通款"
PRD_NEXT_DATE: "20200519"
PRD_TYPE_NAME: "每日型                     "    # 净值类周期型, 活期型
PRED_UNIT: "100.00"
PSUB_UNIT: "100.00"
REALEND_DATE: "20991231"
RED_CLOSE_TIME: "15:30:00"
RISK_LEVEL_NAME: "较低风险(二级)          "
SELLDIR: "个人"
START_DATE: "20140318"
STATUS_NAME: "开放期                     "
'''
# CmbcbackItem仅做备用使用
class CmbcBackItem(Item):
    pattern_rate = re.compile(r'([0-9]+\.?[0-9]*)[%％]*')
    pattern_amount = re.compile(r'[0-9]+\.?[0-9]*')

    pattern_date = re.compile(r'20[0-9]{2}-[01][0-9]-[0123][0-9]')

    bank_name = JsonField(default='民生银行')
    referencable = JsonField(default='较高')

    code = JsonField(json_select='PRD_CODE')
    name = JsonField(json_select='PRD_NAME')
    risk = JsonField(json_select='RISK_LEVEL_NAME')
    currency = JsonField(json_select='CURR_TYPE_NAME')
    term = JsonField(json_select='LIV_TIME_UNIT_NAME')

    date_open = JsonField(json_select='IPO_START_DATE')
    date_close = JsonField(json_select='IPO_END_DATE')
    date_start = JsonField(json_select='START_DATE')
    date_end = JsonField(json_select='REALEND_DATE')

    amount_buy_min = JsonField(json_select='PFIRST_AMT')
    amount_buy_max = JsonField(json_select='PMAX_AMT')
    amount_per_buy = JsonField(json_select='PSUB_UNIT')

    rate_type = JsonField(json_select='PRD_TYPE_NAME')
    rate_min = JsonField(json_select='INCOME_RATE')

    async def clean_code(self, value):
        return value.strip()

    async def clean_name(self, value):
        return value.strip()

    async def clean_risk(self, value):
        res = re.compile(r'[一二三四五六七八九]').search(value)
        if not res:
            return 0
        dig = res.group()
        num = chinese_to_number(dig)
        return num

    async def clean_currency(self, value):
        return value.strip()

    async def clean_term(self, value):
        if not value:
            return 0
        value = value.strip()
        res = re.compile(r'\d+').search(value)
        if not res:
            return 0
        num = int(res.group())
        if '年' in value:
            num = num * 365
        elif '月' in value:
            num = num * 30
        return num

    async def clean_date_open(self, value):
        if not value:
            return ''
        value = value.strip()
        res = re.compile(r'\d{8}').fullmatch(value)
        if not res:
            return ''
        date = res.group()
        time = date[0:4] + '-' + date[4:6] + '-' + date[6:8] + ' 00:00:00'
        return time

    async def clean_date_close(self, value):
        if not value:
            return ''
        value = value.strip()
        res = re.compile(r'\d{8}').fullmatch(value)
        if not res:
            return ''
        date = res.group()
        time = date[0:4] + '-' + date[4:6] + '-' + date[6:8] + ' 00:00:00'
        return time

    async def clean_date_start(self, value):
        if not value:
            return ''
        value = value.strip()
        res = re.compile(r'\d{8}').fullmatch(value)
        if not res:
            return ''
        date = res.group()
        time = date[0:4] + '-' + date[4:6] + '-' + date[6:8] + ' 00:00:00'
        return time

    async def clean_date_end(self, value):
        if not value:
            return ''
        value = value.strip()
        res = re.compile(r'\d{8}').fullmatch(value)
        if not res:
            return ''
        date = res.group()
        time = date[0:4] + '-' + date[4:6] + '-' + date[6:8] + ' 00:00:00'
        return time

    async def clean_amount_buy_min(self, value):
        if not value:
            return 0
        value = value.strip()
        value = value.replace(',', '')
        res = re.compile(r'\d+\.?\d*').search(value)
        if not res:
            return 0
        num = float(res.group())
        if '万' in value:
            num = num * 10000
        return int(num)

    async def clean_amount_buy_max(self, value):
        if not value:
            return 0
        value = value.strip()
        value = value.replace(',', '')
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
        value = value.strip()
        value = value.replace(',', '')
        res = re.compile(r'\d+\.?\d*').search(value)
        if not res:
            return 0
        num = float(res.group())
        if '万' in value:
            num = num * 10000
        return int(num)

    async def clean_rate_type(self, value):
        if not value:
            return 0
        value = value.strip()
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

        value = value.strip()
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


# 先抓取全部理财产品的code, 再分别查询MongoDB数据库中的outline和manual, 如没有，则下载
class CmbcWorker(Spider):
    name = 'CmbcWorker'
    bank_name = '民生银行'
    begin_url = 'https://www.mszxyh.com/peweb/DBFinanceQuotaInfo.do'
    manual_url = 'https://www.mszxyh.com/peweb/PEACInvestPrdIntroQry.do'
    outline_url = 'https://www.mszxyh.com/peweb/DBFinancePrdDetailInfo.do'

    async def start_manual(self):
        form_data = {'QryType': '0'}
        yield self.request(url=self.begin_url, formdata=form_data, callback=self.parse_list)

    async def parse_list(self, response):
        codes_outline = []
        codes_manual = []
        jsondata = await response.json()
        if jsondata:
            list_data = jsondata['List']
            for one in list_data:
                code = one['PrdCode']
                ukey = self.bank_name + '=' + code
                outline = self.collection_outline.find_one({'_id': ukey})
                if not outline:
                    codes_outline.append(code)

                manual = self.collection_manual.find_one({'_id': str(farmhash.hash64(ukey))})
                if not manual:
                    codes_manual.append(code)

        if codes_outline:
            for code in codes_outline:
                formdata = {'PrdCode': code}
                target = Target(bank_name=self.bank_name, method='POST', url=self.outline_url, formdata=formdata)
                await self.redis.insert(field=target.id, value=target.do_dump())

        if codes_manual:
            for code in codes_manual:
                formdata = {"type": "YHLC", "IssueChannelId": "1", "ProdNo": code, "DoType": "3", "FileType": "1"}
                target = Target(bank_name=self.bank_name, method='POST', url=self.manual_url, formdata=formdata, metadata={'code': code}, callback='extract_cmbc_manual')
                await self.redis.insert(field=target.id, value=target.do_dump())


def start():
    CmbcWorker.start()
