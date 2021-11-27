from myspiders.ruia import JsonField, HtmlField, AttrField, TextField, Item, Spider, Bs4TextField, Bs4AttrTextField
import re
from config import WealthOutline
import re
import os


class CbhbItem(Item):
    pattern_name = re.compile(r'渤海银行2020年([\u4E00-\u9FA5\s]+[0-9]+号)([\u4E00-\u9FA5\s]+)理财产品[(（]([0-9A-Z]+D)[）)]')
    pattern_pdf = re.compile(r'/bhbank/S101/attach/2020[0-9A-Z]+D.pdf')
    data = Bs4AttrTextField(target='href', name='a', attrs={'href': pattern_pdf}, url_prefix='http://www.cbhb.com.cn/')


class CbhbWorker(Spider):
    name = 'CbhbWorker'
    bank_name = '渤海银行'

    start_urls = ['http://www.cbhb.com.cn/bhbank/S101/lingshouyinhangfuwu/lcfw/thdxlc/index.htm']
    manual_url_prefix = 'http://www.cbhb.com.cn/bhbank/S101/attach/%s.pdf'

    async def parse(self, response):
        html = await response.text()
        item = await CbhbItem.get_bs4_item(html=html)
        list_data = item.results['data']
        for data in list_data:
            text = data['text']
            manual_url = data['attr']
            if text and len(text) > 20:
                res = CbhbItem.pattern_name.search(text)
                if res:
                    outline = {'manual_url': manual_url}
                    name = res.group(1)
                    temp = res.group(2)
                    code = res.group(3)
                    outline['bank_name'] = '渤海银行'
                    outline['referencable'] = '较高'
                    outline['name'] = name
                    outline['code'] = code

                    if '封闭' in temp:
                        outline['redeem_type'] = '封闭式'
                    elif '开放' in temp:
                        outline['redeem_type'] = '开放式'
                    else:
                        outline['redeem_type'] = ''

                    if '人民币' in temp:
                        outline['currency'] = '人民币'
                    else:
                        outline['currency'] = ''

                    if '非保本' in temp:
                        outline['promise_type'] = '非保本'
                    elif '保本' in temp:
                        outline['promise_type'] = '保本'
                    else:
                        outline['promise_type'] = ''

                    if '浮动' in temp:
                        outline['fixed_type'] = '浮动收益'
                    elif '固定' in temp:
                        outline['fixed_type'] = '固定收益'
                    else:
                        outline['fixed_type'] = ''

                    outline_need = WealthOutline.do_load(outline)
                    await self.save_wealth_outline(outline_need)


def start():
    CbhbWorker.start()

