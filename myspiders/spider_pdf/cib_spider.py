from myspiders.ruia import Spider
import json
from config import WealthOutline
import time
import re
import demjson
from constants.bank_dict import BankDict
from utils.time_util import daytime_standard
from config import Target


class CibWorker(Spider):
    name = 'CibWorker'
    bank_name = '兴业银行'
    start_urls = [
        'https://www.cib.com.cn/cn/personal/wealth-management/xxcx/table',
        'http://directbank.cib.com.cn/hall/show/fin/finlist!ajaxPage.do?dataSet.nd=%s&dataSet.rows=3000&dataSet.page=1' % (int(time.time() * 1000))
    ]

    pattern_one = re.compile(r'yhlcdb=(\[(\s.+)+\s]);')
    pattern_two = re.compile(r'\[.+\]')
    labels = ['name', 'code', 'code_register', 'prd_type', 'issuer', 'risk', 'client', 'area']
    list_risk = BankDict.list_risk

    manual_url_prefix = 'http://directbank.cib.com.cn/hall/show/agrement/queryAgrement.do?name=licai&prodCode='

    pattern_rate = re.compile(r'[0-9]+\.?[0-9]*')

    async def parse(self, response):
        html = await response.text()
        index = html.find('yhlcdb=')
        if index != -1:
            await self.parse_html(response)
        else:
            await self.parse_json(response)

    async def parse_html(self, response):
        print('======================== 开始解析表格 ============================')
        html = await response.text()
        first = self.pattern_one.search(html)
        if first:
            content = first.group(1)
            second = self.pattern_two.findall(content)
            if second:
                for one in second:
                    value = demjson.decode(one)
                    dict_data = dict(zip(self.labels, value))
                    data = {
                        'bank_name': self.bank_name,
                        'referencable': '较高',
                        'name': dict_data['name'],
                        'code': dict_data['code'],
                        'code_register': dict_data['code_register'],
                        'risk': 0,
                        'amount_buy_min': 0,
                        'currency': '',
                        'rate_type': '',
                        'term': 0,
                        'date_open': '',
                        'date_close': '',
                        'date_start': '',
                        'rate_min': 0.0,
                        'rate_max': 0.0,
                        'manual_url': self.manual_url_prefix + dict_data['code'],
                    }
                    risk = dict_data['risk']
                    if risk in self.list_risk.keys():
                        data['risk'] = self.list_risk[risk]
                    print(data)
                    await self.fetch_manual(data)

    async def parse_json(self, response):
        jsondata = await response.json()
        list_data = jsondata['rows']
        for one in list_data:
            risk = one['prodRRName']
            res_risk = re.compile(r'\d').search(risk)
            if not res_risk:
                risk = 0
            else:
                risk = int(res_risk.group())

            data = {
                'bank_name': self.bank_name,
                'referencable': '较高',
                'name': one['finName'],
                'code': one['finCode'],
                'code_register': '',
                'risk': risk,
                'amount_buy_min': int(float(one['minAmt'])) if one['minAmt'] else 0,
                'currency': one['currencyName'],
                'rate_type': '预期收益型' if '收益' in one['returnTypeName'] else '净值型',
                'term': int(one['timeLimit']),
                'date_open': daytime_standard(one['sbscrBeginDate']),
                'date_close': daytime_standard(one['sbscrEndDate']),
                'date_start': daytime_standard(one['valueDate']),
                'rate_min': 0.0,
                'rate_max': 0.0,
                'manual_url': self.manual_url_prefix + one['finCode'],
            }

            rate_reference = one['referenceIncome']
            rate_return = one['returnRate']
            if rate_reference:
                res = self.pattern_rate.findall(rate_reference)
                if res:
                    data['rate_min'] = round(float(res[0]) / 100, 6)
                    data['rate_max'] = round(float(res[-1]) / 100, 6)
            if data['rate_min'] == 0.0 or data['rate_max'] == 0.0:
                if rate_return:
                    res = self.pattern_rate.findall(rate_return)
                    if res:
                        data['rate_min'] = round(float(res[0]) / 100, 6)
                        data['rate_max'] = round(float(res[-1]) / 100, 6)
            print(data)
            await self.fetch_manual(data)

    async def fetch_manual(self, data):
        target = Target(bank_name=self.bank_name, url=data['manual_url'], metadata={'data': data})
        await self.redis.insert(field=target.id, value=target.do_dump())


def start():
    CibWorker.start()

