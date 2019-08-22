from urllib.request import urlopen, Request
import pandas as pd
import re

LIVE_DATA_URL = '%shq.%s/rn=%s&list=%s'
P_TYPE = {'http': 'http://', 'ftp': 'ftp://'}
DOMAINS = {'sina': 'sina.com.cn', 'sinahq': 'sinajs.cn',
           'ifeng': 'ifeng.com', 'sf': 'finance.sina.com.cn',
           'vsf': 'vip.stock.finance.sina.com.cn',
           'idx': 'www.csindex.com.cn', '163': 'money.163.com',
           'em': 'eastmoney.com', 'sseq': 'query.sse.com.cn',
           'sse': 'www.sse.com.cn', 'szse': 'www.szse.cn',
           'oss': 'file.tushare.org', 'idxip': '115.29.204.48',
           'shibor': 'www.shibor.org', 'mbox': 'www.cbooo.cn',
           'tt': 'gtimg.cn', 'gw': 'gw.com.cn',
           'v500': 'value500.com', 'sstar': 'stock.stockstar.com',
           'dfcf': 'nufm.dfcfw.com'}
LIVE_DATA_COLS = ['name', 'open', 'pre_close', 'price', 'high', 'low', 'bid', 'ask', 'volume', 'amount',
                  'b1_v', 'b1_p', 'b2_v', 'b2_p', 'b3_v', 'b3_p', 'b4_v', 'b4_p', 'b5_v', 'b5_p',
                  'a1_v', 'a1_p', 'a2_v', 'a2_p', 'a3_v', 'a3_p', 'a4_v', 'a4_p', 'a5_v', 'a5_p', 'date', 'time', 's']


def _random(n=13):
    from random import randint
    start = 10 ** (n - 1)
    end = (10 ** n) - 1
    return str(randint(start, end))


def _code_to_symbol(code):
    '''
        生成symbol代码标志
    '''

    if len(code) != 6:
        return code
    else:
        return 'sh%s' % code if code[:1] in ['5', '6', '9'] or code[:2] in ['11', '13'] else 'sz%s' % code


def get_realtime_quotes(symbols=None):
    """
        获取实时交易数据 getting real time quotes data
       用于跟踪交易情况（本次执行的结果-上一次执行的数据）
    Parameters
    ------
        symbols : string, array-like object (list, tuple, Series).

    return
    -------
        DataFrame 实时交易数据
              属性:0：name，股票名字
            1：open，今日开盘价
            2：pre_close，昨日收盘价
            3：price，当前价格
            4：high，今日最高价
            5：low，今日最低价
            6：bid，竞买价，即“买一”报价
            7：ask，竞卖价，即“卖一”报价
            8：volumn，成交量 maybe you need do volumn/100
            9：amount，成交金额（元 CNY）
            10：b1_v，委买一（笔数 bid volume）
            11：b1_p，委买一（价格 bid price）
            12：b2_v，“买二”
            13：b2_p，“买二”
            14：b3_v，“买三”
            15：b3_p，“买三”
            16：b4_v，“买四”
            17：b4_p，“买四”
            18：b5_v，“买五”
            19：b5_p，“买五”
            20：a1_v，委卖一（笔数 ask volume）
            21：a1_p，委卖一（价格 ask price）
            ...
            30：date，日期；
            31：time，时间；
    """
    symbols_list = ''

    for code in symbols:
        symbols_list += _code_to_symbol(code) + ','

    symbols_list = symbols_list[:-1] if len(symbols_list) > 8 else symbols_list
    request = Request(LIVE_DATA_URL % (P_TYPE['http'], DOMAINS['sinahq'],
                                          _random(), symbols_list))
    text = urlopen(request, timeout=10).read()
    text = text.decode('GBK')
    reg = re.compile(r'\="(.*?)\";')
    data = reg.findall(text)
    regSym = re.compile(r'(?:sh|sz)(.*?)\=')
    syms = regSym.findall(text)
    data_list = []
    syms_list = []
    for index, row in enumerate(data):
        if len(row) > 1:
            data_list.append([astr for astr in row.split(',')][0:33])
            syms_list.append(syms[index])
    if len(syms_list) == 0:
        return None
    df = pd.DataFrame(data_list, columns=LIVE_DATA_COLS)
    df = df.drop('s', axis=1)
    df['code'] = syms_list
    ls = [cls for cls in df.columns if '_v' in cls]
    for txt in ls:
        df[txt] = df[txt].map(lambda x: x[:-2])
    return df
