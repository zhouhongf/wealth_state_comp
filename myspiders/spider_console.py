import os
import sys
import time
from config import CONFIG
from importlib import import_module

# sys.path.append('../')

crawl_list_back = {
    '工商银行': 'icbc',
    '农业银行': 'abchina',
    '中国银行': 'boc',          # 无理财列表，需要从产品说明书中提取
    '建设银行': 'ccb',
    '交通银行': 'bankcomm',
    '邮储银行': 'psbc',

    '招商银行': 'cmbchina',
    '中信银行': 'citicbank',
    '浦发银行': 'spdb',
    '兴业银行': 'cib',
    '广发银行': 'cgbchina',
    '民生银行': 'cmbc',
    '光大银行': 'cebbank',
    '浙商银行': 'czbank',       # 无理财列表
    '平安银行': 'pingan',
    '华夏银行': 'hxb',          # 无理财code
    '渤海银行': 'cbhb',
    '恒丰银行': 'hfbank'
}

crawl_list = {
    '浦发银行': 'spdb',
}

def file_name(file_dir=os.path.join(CONFIG.BASE_DIR, 'myspiders/spider_pdf')):
    bank_alias = crawl_list.values()
    all_files = []
    for file in os.listdir(file_dir):
        if file.endswith('_spider.py') and file.replace('_spider.py', '') in bank_alias:
            all_files.append(file.replace('.py', ''))
    return all_files


def spider_console():
    start = time.time()

    all_files = file_name()
    for spider in all_files:
        spider_module = import_module("myspiders.spider_pdf.{}".format(spider))
        spider_module.start()

    print("【spider_console】 Time costs: {0}".format(time.time() - start))


if __name__ == '__main__':
    spider_console()
