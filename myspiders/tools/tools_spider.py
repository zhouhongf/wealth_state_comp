import aiohttp
import async_timeout
from urllib.parse import quote
from config import Logger, CONFIG

logger = Logger().logger


# 供 client_manual下载PDF时需要cookie使用
splash_url = 'http://%s:8050/execute?lua_source=' % CONFIG.HOST_LOCAL
lua_script = '''
function main(splash, args)
    local myheaders = {}
    myheaders['User-Agent']='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36'
    assert(splash:go{'%s', headers=myheaders})
    assert(splash:wait(2))
    return {
        html = splash:html(),
        cookies = splash:get_cookies(),
    }
end'''

suffix_check = ['.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.pdf', '.html', '.htm', '.shtml', '.shtm', '.zip', '.rar', '.tar', '.bz2', '.7z', '.gz']
suffix_file = ['.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.pdf', '.zip', '.rar', '.tar', '.bz2', '.7z', '.gz']


cookie_url_spdb = 'https://per.spdb.com.cn/bank_financing/financial_product'


async def fetch_bank_cookies(bank_name):
    cookie = ''
    if bank_name == '浦发银行':
        cookie = await fetch_cookies_spdb()

    return cookie


async def fetch_cookies_by_splash(client, url, timeout=15):
    with async_timeout.timeout(timeout):
        try:
            async with client.get(url, timeout=timeout) as response:
                return await response.json()
        except Exception as e:
            logger.exception(url)
            return None


async def fetch_cookies_spdb():
    lua = lua_script % cookie_url_spdb
    url = splash_url + quote(lua)
    async with aiohttp.ClientSession() as client:
        jsondata = await fetch_cookies_by_splash(client, url)
        if not jsondata:
            return None

        cookies = jsondata['cookies']
        if not cookies:
            return None

        query_set = set()
        for cookie in cookies:
            name = cookie['name'].strip()
            value = cookie['value'].strip()
            if name == 'TSPD_101' or name == 'TS01d02f4c' or name == 'WASSESSION':
                query = name + '=' + value + ';'
                query_set.add(query)
            elif name.startswith('Hm_lvt_') or name.startswith('Hm_lpvt_'):
                query = name + '=' + value + ';'
                query_set.add(query)
        cookie_need = ''
        for query in query_set:
            cookie_need += query
        print('获取到的浦发银行Cookie是：%s' % cookie_need)
        return cookie_need
