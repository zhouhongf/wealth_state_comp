from myspiders.ruia import Master, Response
from urllib.parse import urlencode, urlparse, urljoin, quote
import re
import os
from bs4 import BeautifulSoup
import demjson
import json
from copy import copy
from config import WealthOutline, Target, CONFIG, Document
from constants import BankDict
from myspiders.spider_pdf.abchina_spider import AbchinaItem, AbchinaExtraItem
from myspiders.spider_pdf.bankcomm_spider import BankcommItem
from myspiders.spider_pdf.ccb_spider import CcbItem
from myspiders.spider_pdf.cebbank_spider import CebbankItem
from myspiders.spider_pdf.cmbc_spider import CmbcItem
from myspiders.spider_pdf.cmbchina_spider import CmbchinaItem, CmbchinaPDFMobileItem, CmbchinaPDFWebItem
from myspiders.spider_pdf.icbc_spider import IcbcDataTableItem, IcbcItem
from myspiders.spider_pdf.pingan_spider import PinganItem
from myspiders.spider_pdf.spdb_spider import SpdbItem, get_words_from_formdata


class SpiderMaster(Master):
    name = 'SpiderMaster'
    url_num = CONFIG.MASTER_CRAWL_PER_TIME

    async def parse(self, response):
        target: Target = response.metadata['target']
        bank_name = target.bank_name
        if not target.callback:
            bank_alia = BankDict.list_bank_alia[bank_name]
            extract_method = getattr(self, 'extract_' + bank_alia, None)
        else:
            extract_method = getattr(self, target.callback, None)

        if extract_method is not None and callable(extract_method):
            yield extract_method(response, target)
        else:
            self.logger.error('【%s】没有找到相应的方法：extract_方法' % bank_name)

    async def extract_abchina(self, response: Response, target: Target):
        jsondata = await response.json()
        async for item in AbchinaItem.get_json(jsondata=jsondata):
            code = item.results['code']
            result = self.collection_outline.find_one({'_id': target.bank_name + '=' + code})
            if not result:
                url = AbchinaItem.next_url_prefix % code
                target_next = Target(bank_name=target.bank_name, url=url, callback='extract_abchina_next', metadata={'data': item.results})
                await self.redis.insert(field=target_next.id, value=target_next.do_dump())

    async def extract_abchina_next(self, response: Response, target: Target):
        data = target.metadata['data']
        html = await response.text()
        res = AbchinaItem.pattern_manual_url.search(html)
        if not res:
            await self.redis.delete_one(target.id)
        url_short = res.group(2)
        target_url = urljoin(response.url, url_short)
        data['manual_url'] = target_url

        item = await AbchinaExtraItem.get_bs4_item(html=html)
        data_extra = item.results
        data['currency'] = data_extra['currency']
        data['date_start'] = data_extra['date_start']
        data['date_end'] = data_extra['date_end']
        data['risk'] = data_extra['risk']

        outline = WealthOutline.do_load(data)
        await self.save_wealth_outline(outline, target)


    async def extract_bankcomm(self, response: Response, target: Target):
        data_origin = target.metadata['data']
        html = await response.text()
        soup = BeautifulSoup(html, 'lxml')
        list_tr = soup.select('.bx-table-box .change-box table tr')

        data_dict = {}
        for one in list_tr:
            label = one.select_one('th').get_text(strip=True)
            value = one.select_one('td').get_text(strip=True)
            data_dict[label] = value

        data_filter = BankcommItem.filter_every_row(data_dict)
        data_filter['code'] = data_origin['code']
        data_filter['manual_url'] = data_origin['manual_url']

        async for item in BankcommItem.get_json(jsondata=data_filter):
            data = item.results
            outline = WealthOutline.do_load(data)
            await self.save_wealth_outline(outline, target)


    async def extract_ccb(self, response: Response, target: Target):
        jsondata = await response.json(content_type='text/html')
        async for item in CcbItem.get_json(jsondata=jsondata):
            manual_url = item.results['manual_url']
            if manual_url:
                suffix = os.path.splitext(manual_url)[-1]
                if suffix in CONFIG.FILE_SUFFIX:
                    outline = WealthOutline.do_load(item.results)
                    await self.save_wealth_outline(outline, target)
                else:
                    target_next = Target(bank_name=target.bank_name, url=manual_url, callback='extract_ccb_next', metadata={'data': item.results})
                    await self.redis.insert(field=target_next.id, value=target_next.do_dump())
            else:
                outline = WealthOutline.do_load(item.results)
                await self.save_wealth_outline(outline, target)

    async def extract_ccb_next(self, response: Response, target: Target):
        data = target.metadata['data']
        html = await response.text()
        soup = BeautifulSoup(html, 'lxml')
        dom_need = soup.find(name='a', attrs={'href': re.compile(r'\.pdf|\.doc')})
        if dom_need:
            url = dom_need.get('href')
            if not url.startswith('http'):
                url = urljoin('http://www.ccb.com/', url)
            data['manual_url'] = url
        else:
            body = soup.select_one('.content.f14')
            if body:
                document = Document(ukey=target.bank_name + '=' + data['code'], file_type=BankDict.file_type['manual'], file_suffix='.html', content=str(body))
                await self.save_wealth_manual(document, target)
                data['manual_download'] = 'done'
        outline = WealthOutline.do_load(data)
        await self.save_wealth_outline(outline, target)


    async def extract_cebbank(self, response, target: Target):
        rate_type = target.metadata['rate_type']
        html = await response.text()
        async for item in CebbankItem.get_bs4_items(html=html):
            data = item.results
            data['rate_type'] = rate_type
            target = Target(bank_name=target.bank_name, url=data['url_next'], callback='extract_cebbank_next', metadata={'data': data})
            await self.redis.insert(field=target.id, value=target.do_dump())

    async def extract_cebbank_next(self, response: Response, target: Target):
        data = target.metadata['data']
        data.pop('url_next')

        url_old = response.url
        url_path = urlparse(url_old).path
        url_prefix = os.path.split(url_path)[0]
        pattern_pdf = re.compile(url_prefix + r'/.+\.pdf')

        html = await response.text()
        soup = BeautifulSoup(html, 'lxml')
        a = soup.find(name='a', attrs={'href': pattern_pdf})
        if a:
            url = a.get('href')
            if not url.startswith('http'):
                url = urljoin(url_old, url)
            data['manual_url'] = url
            outline = WealthOutline.do_load(data)
            await self.save_wealth_outline(outline, target)
        else:
            manual_body = soup.select_one('#main_con .cpsms_nr .sms_nr')
            list_p = manual_body.find_all(name='p')
            # 如果p标签大于20个，则将manual_body直接保存进数据库
            if list_p and len(list_p) > 20:
                document = Document(ukey=target.bank_name + '=' + data['code'], file_type=BankDict.file_type['manual'], file_suffix='.html', content=manual_body.prettify())
                await self.save_wealth_manual(document, target)
                data['manual_download'] = 'done'
                outline = WealthOutline.do_load(data)
                await self.save_wealth_outline(outline, target)
            else:
                await self.redis.delete_one(target.id)

    async def extract_cib(self, response: Response, target: Target):
        data = target.metadata['data']
        html = await response.text()
        soup = BeautifulSoup(html, 'lxml')
        content = soup.select_one('#content')
        if content:
            document = Document(ukey=target.bank_name + '=' + data['code'], file_type=BankDict.file_type['manual'], file_suffix='.html', content=content.prettify())
            await self.save_wealth_manual(document, target)
            data['manual_download'] = 'done'
        outline = WealthOutline.do_load(data)
        await self.save_wealth_outline(outline, target)

    async def extract_citicbank(self, response: Response, target: Target):
        data = target.metadata['data']
        html = await response.text()
        document = Document(ukey=target.bank_name + '=' + data['code'], file_type=BankDict.file_type['manual'], file_suffix='.html', content=html)
        await self.save_wealth_manual(document, target)
        data['manual_download'] = 'done'
        outline = WealthOutline.do_load(data)
        await self.save_wealth_outline(outline, target)

    async def extract_cmbc(self, response: Response, target: Target):
        jsondata = await response.json()
        async for item in CmbcItem.get_json(jsondata=jsondata):
            data = item.results
            outline = WealthOutline.do_load(data)
            await self.save_wealth_outline(outline, target)

    async def extract_cmbc_manual(self, response: Response, target: Target):
        code = target.metadata['code']
        jsondata = await response.json()
        content = jsondata['Content']
        if content:
            manual = Document(ukey=target.bank_name + '=' + code, file_type=BankDict.file_type['manual'], file_suffix='.html', content=content)
            await self.save_wealth_manual(manual, target)
        else:
            await self.redis.delete_one(field=target.id)


    async def extract_cmbchina(self, response: Response, target: Target):
        data = target.metadata['data']

        domain_mobile = CmbchinaPDFMobileItem.domain_mobile
        url_old = response.url
        html = await response.text()
        # 先抓取手机链接的PDF文件，如果没有抓取到，则从Web端抓取
        if domain_mobile in url_old:
            item_pdf = await CmbchinaPDFMobileItem.get_item(html=html)
        else:
            item_pdf = await CmbchinaPDFWebItem.get_item(html=html)

        manual_url = item_pdf.results['url']
        if manual_url:
            data['manual_url'] = manual_url
            outline = WealthOutline.do_load(data)
            await self.save_wealth_outline(outline, target)
        else:
            if domain_mobile in url_old and int(target.fails) == 5:
                # 如果没有解析到，并且之前使用的手机链接地址，而且已经失败了5次，则将url换成web请求地址，再请求
                url_web = CmbchinaPDFWebItem.manual_url_prefix_web % data['code']
                target_next = Target(bank_name=target.bank_name, url=url_web, metadata={'data': data})
                await self.redis.insert(field=target_next.id, value=target_next.do_dump())


    async def extract_hfbank(self, response: Response, target: Target):
        data = target.metadata['data']
        html = await response.text(encoding='utf-8')
        soup = BeautifulSoup(html, 'lxml')
        content = soup.select_one('.Section1')
        if content:
            document = Document(ukey=target.bank_name + '=' + data['code'], file_type=BankDict.file_type['manual'], file_suffix='.html', content=content.prettify())
            await self.save_wealth_manual(document, target)
            data['manual_download'] = 'done'
        outline = WealthOutline.do_load(data)
        await self.save_wealth_outline(outline, target)


    async def extract_icbc(self, response: Response, target: Target):
        html = await response.text()
        data_table_item = await IcbcDataTableItem.get_bs4_item(html=html)
        data_table_html = data_table_item.results['data_table']
        async for item in IcbcItem.get_bs4_items(soup=data_table_html):
            data = item.results
            outline = WealthOutline.do_load(data)
            await self.save_wealth_outline(outline, target)


    async def extract_pingan(self, response: Response, target: Target):
        jsondata = await response.json()
        async for item in PinganItem.get_json(jsondata=jsondata):
            data = item.results
            code = data['code']
            result = self.collection_outline.find_one({'_id': target.bank_name + '=' + code})
            if not result:
                url = PinganItem.start_url_next_prefix % code
                referer = PinganItem.next_referer_prefix % code
                headers = copy(target.headers)
                headers.update({'Referer': referer})
                target_next = Target(bank_name=target.bank_name, url=url, headers=headers, callback='extract_pingan_next', metadata={'list_one': data})
                await self.redis.insert(field=target_next.id, value=target_next.do_dump())

    async def extract_pingan_next(self, response: Response, target: Target):
        list_one = target.metadata['list_one']
        jsondata = await response.json()
        list_file = jsondata['data']['instructionFileList']
        for one in list_file:
            attach_type = one['attachType']
            prd_code = one['prdCode']
            if attach_type == '3' and prd_code == list_one['code']:
                list_one['manual_url'] = one['url']
                break

        prd_info = jsondata['data']['prdInfo']
        currency = prd_info['currency']
        list_one['currency'] = PinganItem.currency_transfer[currency]

        rate_range = prd_info['indexContent']
        results = re.compile(self.pattern_rate).findall(rate_range)
        if results:
            rate_min = float(results[0])
            rate_max = float(results[-1])
            if '%' in rate_range or '％' in rate_range:
                rate_min = round(rate_min / 100, 6)
                rate_max = round(rate_max / 100, 6)
            list_one['rate_min'] = max(rate_min, rate_max)
            list_one['rate_max'] = min(rate_min, rate_max)
        print('====================== ', list_one)
        outline = WealthOutline.do_load(list_one)
        await self.save_wealth_outline(outline, target)


    async def extract_spdb(self, response: Response, target: Target):
        form_data = target.metadata['form_data']
        text = await response.text()
        if text.strip():
            jsondata = json.loads(text.strip())
            currency, rate_type = get_words_from_formdata(form_data)
            data_list = jsondata['rows']
            async for item in SpdbItem.get_json(jsondata=data_list):
                data = item.results
                data['currency'] = currency
                data['rate_type'] = rate_type
                print('=============== ', data)
                outline = WealthOutline.do_load(data)
                await self.save_wealth_outline(outline, target)


def start():
    SpiderMaster.start()
