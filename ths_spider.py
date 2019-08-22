# import xml.dom.minidom
# import sqlite3
# from utility import *
# from sqlalchemy import create_engine
#
# import json
# import urllib.request
# import os
# import lxml
#
# import tushare as ts
# import time
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# # from selenium.webdriver.common.by import By
#
# from multiprocessing import Process
#
#
# def get_cookie():
#     url = 'http://www.iwencai.com/'
#     chrome_options = Options()
#     # chrome_options.add_argument('--headless')
#     chrome_options.add_argument('--disable-gpu')
#     chrome_options.add_argument('blink-settings=imagesEnabled=false')
#     driver = webdriver.Chrome(options=chrome_options)
#     driver.get(url)
#     input("请登陆后按Enter")
#     cookie = {}
#     for i in driver.get_cookies():
#         cookie[i["name"]] = i["value"]
#     with open("wencai_cookie.txt", "w") as f:
#         f.write(json.dumps(cookie))
#
#
# def download_stock_change():
#     cookie = {}
#     with open("wencai_cookie.txt") as f:
#         cookie = json.load(f)
#     url_home = 'http://www.iwencai.com'
#     query_url = 'http://www.iwencai.com/stockpick/search?typed=1&preParams=&ts=1&f=3&qs=result_rewrite&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w=最近8个月增减持计划&queryarea='
#     chrome_options = Options()
#     # chrome_options.add_argument('--headless')
#     chrome_options.add_argument('--disable-gpu')
#     chrome_options.add_argument('blink-settings=imagesEnabled=false')
#     driver = webdriver.Chrome(options=chrome_options)
#     driver.get(url_home)
#     # input('press enter to continue')
#     for key in cookie:
#         driver.add_cookie({'name': key, 'value': cookie[key]})
#
#     driver.get(query_url)
#     download_path = '/Users/hero101/Downloads'
#     file_name = datetime.date.today().strftime('%Y-%m-%d.xls')
#     full_file_name = '%s/%s' % (download_path, file_name)
#     if os.path.exists(full_file_name):
#         os.remove(full_file_name)
#     driver.find_element_by_xpath("//div[@id='table_top_bar']//ul/li[contains(text(),'导数据')]").click();
#     time.sleep(5)
#     dest_file = 'zenjianchi.xml'
#     if os.path.exists(dest_file):
#         os.remove(dest_file)
#     os.rename(full_file_name, dest_file)
#     driver.close()
#
#
# #   业绩预告处理
# def performance(file_name, table_name):
#     with open(file_name) as f:
#         root = lxml.etree.HTML(f.read())
#         items = root.xpath('//tr')
#
#         conn = sqlite3.connect('raw_data.db')
#         cursor = conn.cursor()
#         cursor.execute("drop table if exists '%s'" % table_name)
#         cursor.execute(
#             "create table if not exists '%s' (code TEXT,name TEXT,type TEXT,profit real,profit_pre real,profit_percent real,notice_date TEXT,reason TEXT)" % table_name)
#         insert_sql = "insert into '%s' values (?,?,?,?,?,?,?,?)" % table_name
#
#         for i in range(1, len(items)):
#             tds = items[i].xpath('./td/text()')
#             code = tds[0].strip()[0:-3]
#             name = tds[1].strip()
#             type = tds[4].strip()
#             profit = tds[5].strip()
#             profit_pre = tds[6].strip()
#             profit_percent = tds[7].strip()
#             notice_date = tds[8].strip()
#             reason = tds[9].strip()
#
#             cursor.execute(insert_sql, (
#                 code, name, type, 0 if profit == '--' else float(profit),
#                 0 if profit_pre == '--' else float(profit_pre),
#                 0 if profit_percent == '--' else float(profit_percent), notice_date, reason))
#         conn.commit()
#
#
# #  增减持统计
# def stockchange(file_name, table_name):
#     with open(file_name) as f:
#         conn = sqlite3.connect('raw_data.db')
#         cursor = conn.cursor()
#         cursor.execute("drop table if exists '%s'" % table_name)
#         cursor.execute(
#             "create table if not exists '%s' (code TEXT,name TEXT,first_date TEXT,holder_type TEXT,dir TEXT,process TEXT,last_date TEXT,start_date TEXT,end_date TEXT,amount real,percent real)" % table_name)
#         insert_sql = "insert into '%s' values (?,?,?,?,?,?,?,?,?,?,?)" % table_name
#
#         root = lxml.etree.HTML(f.read())
#         items = root.xpath('//tr')
#         for i in range(1, len(items)):
#             tds = items[i].xpath('./td/text()')
#             code = tds[0].strip()[0:-3]
#             name = tds[1].strip()
#             first_date = tds[4].strip()
#             holder_type = tds[6].strip()
#             dir = tds[7].strip()
#             process = tds[8].strip()
#             last_date = tds[9].strip()
#             start_date = tds[10].strip()
#             end_date = tds[11].strip()
#             amount = tds[14].strip()
#             percent = tds[15].strip()
#             cursor.execute(insert_sql, (
#                 code, name, first_date, holder_type, dir, process, last_date, start_date, end_date,
#                 0 if amount == '--' else float(amount), 0 if percent == '--' else float(percent)))
#         conn.commit()
#
#
# #  增减持统计 废弃
# def stockchange_old(file_name, table_name):
#     document_tree = xml.dom.minidom.parse(file_name)
#     items = document_tree.getElementsByTagName("tr")
#     conn = sqlite3.connect('raw_data.db')
#     cursor = conn.cursor()
#     cursor.execute("drop table if exists '%s'" % table_name)
#     cursor.execute(
#         "create table if not exists '%s' (code TEXT,name TEXT,first_date TEXT,holder_type TEXT,dir TEXT,process TEXT,last_date TEXT,start_date TEXT,end_date TEXT,amount real,percent real)" % table_name)
#     insert_sql = "insert into '%s' values (?,?,?,?,?,?,?,?,?,?,?)" % table_name
#
#     for i in range(1, len(items)):
#         code = items[i].childNodes[0].childNodes[0].data.strip()[0:-3]
#         name = items[i].childNodes[1].childNodes[0].data.strip()
#         first_date = items[i].childNodes[4].childNodes[0].data.strip()
#         holder_type = items[i].childNodes[6].childNodes[0].data.strip()
#         dir = items[i].childNodes[7].childNodes[0].data.strip()
#         process = items[i].childNodes[8].childNodes[0].data.strip()
#         last_date = items[i].childNodes[9].childNodes[0].data.strip()
#         start_date = items[i].childNodes[10].childNodes[0].data.strip()
#         end_date = items[i].childNodes[11].childNodes[0].data.strip()
#         amount = items[i].childNodes[14].childNodes[0].data.strip()
#         percent = items[i].childNodes[15].childNodes[0].data.strip()
#         cursor.execute(insert_sql, (
#             code, name, first_date, holder_type, dir, process, last_date, start_date, end_date,
#             0 if amount == '--' else float(amount), 0 if percent == '--' else float(percent)))
#     conn.commit()
#
#
# #   业绩预告处理  废弃
# def performance_old(file_name, table_name):
#     document_tree = xml.dom.minidom.parse(file_name)
#     items = document_tree.getElementsByTagName("tr")
#     conn = sqlite3.connect('raw_data.db')
#     cursor = conn.cursor()
#     cursor.execute("drop table if exists '%s'" % table_name)
#     cursor.execute(
#         "create table if not exists '%s' (code TEXT,name TEXT,type TEXT,profit real,profit_pre real,profit_percent real,notice_date TEXT,reason TEXT)" % table_name)
#     insert_sql = "insert into '%s' values (?,?,?,?,?,?,?,?)" % table_name
#
#     for i in range(1, len(items)):
#         code = items[i].childNodes[0].childNodes[0].data.strip()[0:-3]
#         name = items[i].childNodes[1].childNodes[0].data.strip()
#         type = items[i].childNodes[4].childNodes[0].data.strip()
#         profit = items[i].childNodes[5].childNodes[0].data.strip()
#         profit_pre = items[i].childNodes[6].childNodes[0].data.strip()
#         profit_percent = items[i].childNodes[7].childNodes[0].data.strip()
#         notice_date = items[i].childNodes[8].childNodes[0].data.strip()
#         reason = items[i].childNodes[9].childNodes[0].data.strip()
#
#         cursor.execute(insert_sql, (
#             code, name, type, 0 if profit == '--' else float(profit), 0 if profit_pre == '--' else float(profit_pre),
#             0 if profit_percent == '--' else float(profit_percent), notice_date, reason))
#     conn.commit()
#
#     pass
#
#
# def create_txt():
#     conn = sqlite3.connect('raw_data.db')
#     cursor = conn.cursor()
#     cursor.execute("select distinct code from yuzen where type='扭亏' order by profit_percent desc ")
#     u_write_to_file('/Users/hero101/Documents/t_niukui.txt', [row[0] for row in cursor])
#     cursor.execute("select distinct code from yuzen where type='大幅上升' order by profit_percent desc ")
#     u_write_to_file('/Users/hero101/Documents/t_dfss.txt', [row[0] for row in cursor])
#
#     cursor.execute("select distinct code from yuzen where type='预增' order by profit_percent desc ")
#     u_write_to_file('/Users/hero101/Documents/t_yuzen.txt', [row[0] for row in cursor])
#
#     cursor.execute("select distinct code from yujian where type='预减' order by profit_percent")
#     u_write_to_file('/Users/hero101/Documents/t_yuzen.txt', [row[0] for row in cursor])
#     cursor.execute("select distinct code from yujian where type='大幅下降' order by profit_percent")
#     u_write_to_file('/Users/hero101/Documents/t_dfxj.txt', [row[0] for row in cursor])
#
#     cursor.execute(
#         "select distinct code from zenjianchi where dir = '减持' and process == '进行中' and percent > 0.1 and end_date > '%s' and first_date > '%s'" % (
#             u_month_befor(
#                 0), u_month_befor(3)))
#     u_write_to_file('/Users/hero101/Documents/t_jianchi.txt', [row[0] for row in cursor])
#
#     cursor.execute(
#         "select distinct code from zenjianchi where dir = '增持' and process == '进行中' and end_date > '%s' and first_date > '%s'" % (
#             u_month_befor(
#                 0), u_month_befor(3)))
#     u_write_to_file('/Users/hero101/Documents/t_zenchi.txt', [row[0] for row in cursor])
#
#
# def fina_ths_process(dbname, codes, names):
#     if codes:
#         chrome_options = Options()
#         chrome_options.add_argument('--headless')
#         chrome_options.add_argument('--disable-gpu')
#         chrome_options.add_argument('blink-settings=imagesEnabled=false')
#         driver = webdriver.Chrome(options=chrome_options)
#
#         conn = sqlite3.connect(dbname)
#         table_name = 'finance'
#         cursor = conn.cursor()
#         cursor.execute("drop table if exists '%s'" % table_name)
#         cursor.execute(
#             "create table if not exists '%s' (code TEXT,name TEXT,report_date TEXT,net_profit TEXT,net_profit_rate TEXT,kf_net_profit TEXT,kf_net_profit_rate TEXT,revenue TEXT,revenue_rate TEXT)" % table_name)
#         insert_sql = "insert into '%s' values (?,?,?,?,?,?,?,?,?)" % table_name
#         index = 0
#         for code in codes:
#             print(code)
#             # 'http://stockpage.10jqka.com.cn/%s/finance/'
#             fina_url = 'http://basic.10jqka.com.cn/%s/finance.html' % code
#             # fina_url = 'http://stockpage.10jqka.com.cn/%s/finance/' % code
#             driver.get(fina_url)
#             # driver.switch_to.frame('dataifm')
#             root = lxml.etree.HTML(driver.page_source)
#             report_date = root.xpath('//div[@class="data_tbody"]//table[@class="top_thead"]//th/div/text()')
#             items = root.xpath('//div[@class="data_tbody"]//table[@class="tbody"]//tr')
#             # analyze = root.xpath('//div[@class="courier_right"]/p/text()')
#             # print(analyze)
#             # cursor.execute(insert_sql, (
#             #     k, fields[0], fields[1], fields[2], ','.join(conecpt), ','.join(conecpt_e), ','.join(conecpt_o)))
#             if items:
#                 net_profit = items[1].xpath('./td/text()')
#                 net_profit_rate = items[2].xpath('./td/text()')
#                 kf_net_profit = items[3].xpath('./td/text()')
#                 kf_net_profit_rate = items[4].xpath('./td/text()')
#                 revenue = items[5].xpath('./td/text()')
#                 revenue_rate = items[6].xpath('./td/text()')
#
#                 length = min(len(net_profit), len(report_date), 21)
#                 for i in range(length):
#                     cursor.execute(insert_sql, (
#                         code, names[index], report_date[i], net_profit[i], net_profit_rate[i], kf_net_profit[i],
#                         kf_net_profit_rate[i], revenue[i], revenue_rate[i]))
#
#             index += 1
#         conn.commit()
#         driver.close()
#
#
# def fina_ths():
#     import os
#     import time
#     pro = ts.pro_api()
#     df = pro.stock_basic()
#     codes = [row['symbol'] for index, row in df.iterrows()]
#     names = [row['name'] for index, row in df.iterrows()]
#
#     process_per = 450
#     process_count = int(len(codes) / process_per + 1)
#     # p = Pool(process_count)
#     dbs = []
#     pp = []
#     for index in range(process_count):
#         db_name = 'fina_%s.db' % index
#         dbs.append(db_name)
#         name = names[index * process_per:(index + 1) * process_per]
#         code = codes[index * process_per:(index + 1) * process_per]
#         p = Process(target=fina_ths_process, args=(db_name, code, name,))
#         p.start()
#         pp.append(p)
#
#     # p.close()
#     # p.join()
#
#     for p in pp:
#         p.join()
#
#     conn = sqlite3.connect('fina_ths')
#     table_name = 'finance'
#     cursor = conn.cursor()
#     cursor.execute("drop table if exists '%s'" % table_name)
#     cursor.execute(
#         "create table if not exists '%s' (code TEXT,name TEXT,report_date TEXT,net_profit TEXT,net_profit_rate TEXT,kf_net_profit TEXT,kf_net_profit_rate TEXT,revenue TEXT,revenue_rate TEXT)" % table_name)
#     insert_sql = "insert into '%s' values (?,?,?,?,?,?,?,?,?)" % table_name
#
#     for db in dbs:
#         if os.path.exists(db):
#             conn_src = sqlite3.connect(db)
#             cursor_src = conn_src.cursor()
#             cursor_src.execute("select * from '%s'" % table_name)
#             for row in cursor_src:
#                 cursor.execute(insert_sql, row)
#             conn_src.close()
#             os.remove(db)
#         else:
#             print(db, ' not exist!!')
#
#     conn.commit()
#     cursor.execute("create index if not exists 'fina_code_date' on '%s'(code,report_date)" % table_name)
#
#     print('All subprocesses done.')
#
#
# def tonghuashun1():
#     req_headers = {
#         'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36',
#         'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
#         'Cookie': 'searchGuide=sg; usersurvey=1; isTradeDay=yes; reviewJump=nojump',
#     }
#
#     text = ''''''
#
#     # htmlDiv = lxml.etree.HTML(text)  # 构造了一个XPath解析对象并对HTML文本进行自动修正。
#     # # title = htmls.xpath("//meta[1]/@content")
#     url = 'http://stockpage.10jqka.com.cn/603106/finance/'
#     url = 'http://basic.10jqka.com.cn/603106/finance.html'
#     req = urllib.request.Request(url, headers=req_headers)
#     response = urllib.request.urlopen(req).read()
#     root = lxml.etree.HTML(response)
#     root = lxml.etree.parse('tonghuashun.html', lxml.etree.HTMLParser()).getroot()
#     # root = BeautifulSoup(response, 'html.parser')
#     res = root.cssselect('div.data_tbody table.tbody tr')
#     from lxml.etree import fromstring
#     h = fromstring('''<div id="outer">
#     ...   <div id="inner" class="content_body">
#     ...       好的
#     ...   </div></div>''')
#     r1 = h.cssselect('div.content_body')
#     print(res)
#
#
# def rate_text_to_float(rate):
#     if rate == '--':
#         return 0
#     return float(rate[:-1])
#
#
# def amount_text_to_float(amount):
#     if amount == '--':
#         return 0
#     amount_num = amount[:-1]
#     amount_unit = amount[-1:]
#     if amount_unit == '万':
#         return round(float(amount_num) / 10000, 6)
#     elif amount_unit == '亿':
#         if amount_num[-1:] == '万':
#             return float(amount_num[:-1]) * 10000
#         else:
#             return float(amount_num)
#     return 0
#
#
# def analyze_finance():
#     conn = sqlite3.connect('fina_ths')
#     fina_table_name = 'finance'
#     cursor = conn.cursor()
#     cursor.execute("select distinct code from '%s'" % fina_table_name)
#     codes = [row[0] for row in cursor]
#
#     analyze_table_name = 'analyze_finance'
#     cursor.execute("drop table if exists '%s'" % analyze_table_name)
#     cursor.execute(
#         '''create table if not exists '%s' (
#                 code TEXT,
#                 name TEXT,
#                 lrd TEXT,
#                 lrvr real,
#                 lnpr real,
#                 lkfnpr real,
#                 rv real,
#                 np real,
#                 kfnp real,
#                 rvr1 real,
#                 rvr2 real,
#                 rvr3 real,
#                 rvr4 real,
#                 npr1 real,
#                 npr2 real,
#                 npr3 real,
#                 npr4 real,
#                 kfnpr1 real,
#                 kfnpr2 real,
#                 kfnpr3 real,
#                 kfnpr4 real,
#                 q1rv TEXT,
#                 q1np TEXT,
#                 q1kfnp TEXT,
#                 q2rv TEXT,
#                 q2np TEXT,
#                 q2kfnp TEXT,
#                 q3rv TEXT,
#                 q3np TEXT,
#                 q3kfnp TEXT )''' % analyze_table_name)
#     insert_sql = "insert into '%s' values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" % analyze_table_name
#
#     for code in codes:
#         cursor.execute("select * from '%s' where code = '%s' order by report_date desc" % (fina_table_name, code))
#         # 最后报告期
#         lrd = ''
#         # 最后报告期 营收同比增长率
#         lrvr = 0
#         # 最后报告期 净利润同比增长率
#         lnpr = 0
#         # 最后报告期 扣非净利润同比增长率
#         lkfnpr = 0
#         name = ''
#         # 最近一年的营收、净利润、扣非净利润
#         rv = 0
#         np = 0
#         kfnp = 0
#         # 最近5年的营收增长率
#         rvr1 = 0
#         rvr2 = 0
#         rvr3 = 0
#         rvr4 = 0
#         # 最近5年的利润增长率
#         npr1 = 0
#         npr2 = 0
#         npr3 = 0
#         npr4 = 0
#         # 最近5年的扣非利润增长率
#         kfnpr1 = 0
#         kfnpr2 = 0
#         kfnpr3 = 0
#         kfnpr4 = 0
#
#         quater1 = {}
#         quater2 = {}
#         quater3 = {}
#         quater4 = {}
#
#         first_row = True
#         for row in cursor:
#             if first_row:
#                 lrd = row[2]
#                 name = row[1]
#                 lnpr = rate_text_to_float(row[4])
#                 lkfnpr = rate_text_to_float(row[6])
#                 lrvr = rate_text_to_float(row[8])
#                 first_row = False
#             year = row[2][:4]
#             q_date = row[2][-5:]
#             if q_date == '03-31':
#                 quater1[year] = (row[3], row[4], row[5], row[6], row[7], row[8])
#             elif q_date == '06-30':
#                 quater2[year] = (row[3], row[4], row[5], row[6], row[7], row[8])
#             elif q_date == '09-30':
#                 quater3[year] = (row[3], row[4], row[5], row[6], row[7], row[8])
#             elif q_date == '12-31':
#                 quater4[year] = (row[3], row[4], row[5], row[6], row[7], row[8])
#                 lenq4 = len(quater4)
#                 if lenq4 == 1:
#                     rv = amount_text_to_float(row[7])
#                     np = amount_text_to_float(row[3])
#                     kfnp = amount_text_to_float(row[5])
#                     rvr1 = rate_text_to_float(row[8])
#                     npr1 = rate_text_to_float(row[4])
#                     kfnpr1 = rate_text_to_float(row[6])
#                 elif lenq4 == 2:
#                     rvr2 = rate_text_to_float(row[8])
#                     npr2 = rate_text_to_float(row[4])
#                     kfnpr2 = rate_text_to_float(row[6])
#                 elif lenq4 == 3:
#                     rvr3 = rate_text_to_float(row[8])
#                     npr3 = rate_text_to_float(row[4])
#                     kfnpr3 = rate_text_to_float(row[6])
#                 elif lenq4 == 4:
#                     rvr4 = rate_text_to_float(row[8])
#                     npr4 = rate_text_to_float(row[4])
#                     kfnpr4 = rate_text_to_float(row[6])
#
#         now_year = datetime.date.today().year
#         # 只计算5年数据
#
#         # 1季度 营收、利润、扣非利润最近5年走势
#         q1rv = []
#         q1np = []
#         q1kfnp = []
#         # 2季度 营收、利润、扣非利润走势
#         q2rv = []
#         q2np = []
#         q2kfnp = []
#         # 3季度 营收、利润、扣非利润走势
#         q3rv = []
#         q3np = []
#         q3kfnp = []
#
#         for year in range(now_year, now_year - 5, -1):
#             year_text = '%s' % year
#             if year_text in quater1:
#                 q1rv.append(str(rate_text_to_float(quater1[year_text][5])))
#                 q1np.append(str(amount_text_to_float(quater1[year_text][0])))
#                 q1kfnp.append(str(amount_text_to_float(quater1[year_text][2])))
#             else:
#                 q1rv.append('0')
#                 q1np.append('0')
#                 q1kfnp.append('0')
#
#             if year_text in quater2:
#                 q2rv.append(str(rate_text_to_float(quater2[year_text][5])))
#                 q2np.append(str(amount_text_to_float(quater2[year_text][0])))
#                 q2kfnp.append(str(amount_text_to_float(quater2[year_text][2])))
#             else:
#                 q2rv.append('0')
#                 q2np.append('0')
#                 q2kfnp.append('0')
#
#             if year_text in quater3:
#                 q3rv.append(str(rate_text_to_float(quater3[year_text][5])))
#                 q3np.append(str(amount_text_to_float(quater3[year_text][0])))
#                 q3kfnp.append(str(amount_text_to_float(quater3[year_text][2])))
#             else:
#                 q3rv.append('0')
#                 q3np.append('0')
#                 q3kfnp.append('0')
#             pass
#
#         cursor.execute(insert_sql, (
#             code, name, lrd, lrvr, lnpr, lkfnpr, rv, np, kfnp, rvr1, rvr2, rvr3, rvr4, npr1, npr2, npr3, npr4, kfnpr1,
#             kfnpr2, kfnpr3, kfnpr4, '--'.join(q1rv), '--'.join(q1np), '--'.join(q1kfnp), '--'.join(q2rv),
#             '--'.join(q2np),
#             '--'.join(q2kfnp), '--'.join(q3rv), '--'.join(q3np), '--'.join(q3kfnp)))
#
#     conn.commit()
#
#     pass
#
#
# def forecast(days):
#     engine = create_engine('sqlite:///fundamental.db' )
#
#     pro = ts.pro_api()
#     for i in range(days):
#         ann_date = u_day_befor(i)
#         print(ann_date)
#         df = pro.forecast(ann_date=ann_date)
#         df.to_sql('forecast', engine, if_exists='append', index=True)
#     # df = pro.stock_basic()
#     # codes = [row['symbol'] for index, row in df.iterrows()]
#     # ts_codes = [row['ts_code'] for index, row in df.iterrows()]
#     # names = [row['name'] for index, row in df.iterrows()]
#     #
#     # df = pro.forecast(ann_date='20190415')
#     # print(df)
#     # for ts_code in ts_codes:
#     #     print(ts_code)
#     #     df = pro.forecast(ts_code=ts_code)
#     #     # print(df)
#
#
# if __name__ == '__main__':
#     import cProfile
#
#     # performance('yujian.xml', 'yujian')
#     # performance('yuzen.xml', 'yuzen')
#     # tonghuashun2('zenjianchi.xml', 'zenjianchi')
#     # stockchange('zenjianchi.xml', 'zenjianchi')
#     # df = ts.forecast_data(2019, 2)
#     # print(df)
#     # create_txt()
#     # analyze_finance()
#     # download_stock_change()
#     # get_cookie()
#     forecast(120)
#     pass
