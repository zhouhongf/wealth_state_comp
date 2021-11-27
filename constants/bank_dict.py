
class BankDict:

    file_type = {
        'outline': '理财列表',
        'manual': '产品说明书',
        'notice_issue': '产品发行公告',
        'notice_start': '产品成立公告',
        'notice_end': '产品终止公告',
    }

    list_bank_alia = {
        '工商银行': 'icbc',
        '农业银行': 'abchina',
        '中国银行': 'boc',
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
        '浙商银行': 'czbank',
        '平安银行': 'pingan',
        '华夏银行': 'hxb',
        '渤海银行': 'cbhb',
        '恒丰银行': 'hfbank',

        '北京银行': 'bankofbeijing',
        '成都银行': 'bocd',
        '贵阳银行': 'bankgy',
        '杭州银行': 'hzbank',
        '江苏银行': 'jsbchina',
        '南京银行': 'njcb',
        '宁波银行': 'nbcb',
        '青岛银行': 'qdccb',
        '上海银行': 'bosc',
        '苏州银行': 'suzhoubank',
        '西安银行': 'xacbank',
        '长沙银行': 'cscb',
        '郑州银行': 'zzbank',

        '常熟银行': 'csrcbank',
        '江阴银行': 'jybank',
        '青农银行': 'qrcb',
        '苏农银行': 'szrcb',
        '无锡银行': 'wrcb',
        '张家港行': 'zrcbank',
        '紫金银行': 'zjrcbank'
    }

    bank_file_types_manual = {
        '工商银行': ['.pdf'],
        '农业银行': ['.pdf'],
        '中国银行': ['.pdf'],
        '建设银行': ['.pdf', '.doc', '.html'],
        '交通银行': ['.pdf'],
        '邮储银行': ['.pdf'],

        '招商银行': ['.pdf'],
        '中信银行': ['.html'],
        '浦发银行': ['.pdf'],
        '兴业银行': ['.html'],
        '广发银行': ['.pdf'],
        '民生银行': ['.html'],
        '光大银行': ['.pdf', '.html'],
        '浙商银行': ['.pdf'],
        '平安银行': ['.pdf'],
        '华夏银行': ['.pdf'],
        '渤海银行': ['.pdf'],
        '恒丰银行': ['.html'],

        '北京银行': ['.pdf'],
        '成都银行': ['.pdf'],
        '贵阳银行': ['.pdf'],
        '杭州银行': ['.pdf'],
        '江苏银行': ['.pdf'],
        '南京银行': ['.pdf'],
        '宁波银行': ['.pdf'],
        '青岛银行': ['.pdf'],
        '上海银行': ['.pdf'],
        '苏州银行': ['.pdf'],
        '西安银行': ['.pdf'],
        '长沙银行': ['.pdf'],
        '郑州银行': ['.pdf'],

        '常熟银行': ['.pdf'],
        '江阴银行': ['.pdf'],
        '青农银行': ['.pdf'],
        '苏农银行': ['.html'],
        '无锡银行': ['.pdf'],
        '张家港行': ['.pdf'],
        '紫金银行': ['.html'],
    }


    wealth_base = {
        'base_rate_save': 0.015,
        'base_rate_loan': 0.0435,
    }

    list_risk = {
        '无风险': 0, '基本无风险': 0, '无': 0,
        '低风险': 1, '极低风险': 1, '低': 1, '极低': 1, '谨慎型': 1,
        '较低风险': 2, '中低风险': 2, '较低': 2, '中低': 2, '稳健型': 2,
        '中等风险': 3, '中风险': 3, '中等': 3, '中': 3, '平衡型': 3,
        '较高风险': 4, '中高风险': 4, '较高': 4, '中高': 4, '进取型': 4,
        '高风险': 5, '高': 5, '激进型': 5
    }

    list_currency = {
        '人民币', '美元', '欧元', '日元', '英镑',
    }

    list_bank_level = {
        '工商银行': '国有银行',
        '农业银行': '国有银行',
        '中国银行': '国有银行',
        '建设银行': '国有银行',
        '交通银行': '国有银行',
        '邮储银行': '国有银行',

        '招商银行': '股份银行',
        '中信银行': '股份银行',
        '浦发银行': '股份银行',
        '兴业银行': '股份银行',
        '广发银行': '股份银行',
        '民生银行': '股份银行',
        '光大银行': '股份银行',
        '浙商银行': '股份银行',
        '平安银行': '股份银行',
        '华夏银行': '股份银行',
        '渤海银行': '股份银行',
        '恒丰银行': '股份银行',

        '北京银行': '城商银行',
        '成都银行': '城商银行',
        '贵阳银行': '城商银行',
        '杭州银行': '城商银行',
        '江苏银行': '城商银行',
        '南京银行': '城商银行',
        '宁波银行': '城商银行',
        '青岛银行': '城商银行',
        '上海银行': '城商银行',
        '苏州银行': '城商银行',
        '西安银行': '城商银行',
        '长沙银行': '城商银行',
        '郑州银行': '城商银行',

        '常熟银行': '农商银行',
        '江阴银行': '农商银行',
        '青农银行': '农商银行',
        '苏农银行': '农商银行',
        '无锡银行': '农商银行',
        '张家港行': '农商银行',
        '紫金银行': '农商银行',
    }

    list_promise_type = {
        0: '非保本',
        1: '保本',
    }

    list_raise_type = {
        0: '公募',
        1: '私募',
    }

    list_fixed_type = {
        0: '浮动收益',
        1: '固定收益',
    }

    list_redeem_type = {
        0: '封闭式',
        1: '开放式',
    }

    list_rate_type = {
        0: '净值型',
        1: '预期收益型',
    }


    list_term_type = {
        0: '无固定期限',
        7: '0-7天(含)',
        30: '7-30天(含)',
        90: '30-90天(含)',
        180: '90-180天(含)',
        365: '180-365天(含)',
        1095: '1-3年(含)',
        3650: '3-10年(含)',
        36500: '10年以上',
    }

    list_amount_size_type = {
        1: '0-1亿(含)',
        5: '1-5亿(含)',
        10: '5-10亿(含)',
        50: '10-50亿(含)',
        100: '50-100亿(含)',
        200: '100-200亿(含)',
        500: '200-500亿(含)',
        1000: '500亿以上',
    }

    list_amount_buy_min_type = {
        1: '0-1万(含)',
        5: '1-5万(含)',
        10: '5-10万(含)',
        50: '10-50万(含)',
        100: '50-100万(含)',
        200: '100-200万(含)',
        500: '200-500万(含)',
        1000: '500万以上',
    }

    list_rate = {
        1: '0-1%(含)',
        2: '1-2%(含)',
        3: '2-3%(含)',
        4: '3-4%(含)',
        5: '4-5%(含)',
        6: '5-6%(含)',
        7: '7-8%(含)',
        8: '8-9%(含)',
        9: '9-10%(含)',
        10: '10%以上',
    }
