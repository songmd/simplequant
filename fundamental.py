import sqlite3
from utility import *
from sqlalchemy import create_engine
import json
import urllib.request
import os
import lxml
import tushare as ts
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from multiprocessing import Process
import re


# 数据处理
class Fundamental(object):
    FUNDA_DB = 'data/fundamental.db'
    WENCAI_COOKIE = 'data/wencai_cookie.txt'
    WENCAI_HOME_URL = 'http://www.iwencai.com'
    DOWNLOAD_PATH = '/Users/hero101/Downloads'
    ZENJIANCHI = 'data/zenjianchi.xml'
    YUZENJIAN = 'data/yuzenjian.xml'
    YUZJ_TB = 'yuzenjian'
    ZENJC_TB = 'zenjianchi'
    FINA_TB = 'finance'
    BASIC_TB = 'basic'
    ANAL_FINA_TB = 'analyze_finance'
    INDUSTRY_TB = 'industry'
    CONCEPT_TB = 'concept'

    @staticmethod
    def rate_text_to_float(rate):
        if rate == '--':
            return 0
        return float(rate[:-1])

    @staticmethod
    def amount_text_to_float(amount):
        if amount == '--':
            return 0
        amount_num = amount[:-1]
        amount_unit = amount[-1:]
        if amount_unit == '万':
            return round(float(amount_num) / 10000, 6)
        elif amount_unit == '亿':
            if amount_num[-1:] == '万':
                return float(amount_num[:-1]) * 10000
            else:
                return float(amount_num)
        return 0

    @staticmethod
    def create_wencai_cookie():
        chrome_options = Options()
        # chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('blink-settings=imagesEnabled=false')
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(Fundamental.WENCAI_HOME_URL)
        input("请登陆后按Enter")
        cookie = {}
        for i in driver.get_cookies():
            cookie[i["name"]] = i["value"]
        with open(Fundamental.WENCAI_COOKIE, "w") as f:
            f.write(json.dumps(cookie))

    @staticmethod
    def download_from_wencai():
        download_tasks = [
            {
                'query_url': 'http://www.iwencai.com/stockpick/search?typed=0&preParams=&ts=1&f=1&qs=result_original&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w=业绩预告&queryarea=',
                'dest_file': Fundamental.YUZENJIAN,
                'sleep': 5
            },
            {
                'query_url': 'http://www.iwencai.com/stockpick/search?typed=1&preParams=&ts=1&f=3&qs=result_rewrite&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w=最近8个月增减持计划&queryarea=',
                'dest_file': Fundamental.ZENJIANCHI,
                'sleep': 5
            },

        ]
        with open(Fundamental.WENCAI_COOKIE) as f:
            cookie = json.load(f)
            chrome_options = Options()
            # chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('blink-settings=imagesEnabled=false')

            for task in download_tasks:
                driver = webdriver.Chrome(options=chrome_options)
                driver.get(Fundamental.WENCAI_HOME_URL)
                for key in cookie:
                    driver.add_cookie({'name': key, 'value': cookie[key]})
                driver.get(task['query_url'])
                file_name = datetime.date.today().strftime('%Y-%m-%d.xls')
                full_file_name = '%s/%s' % (Fundamental.DOWNLOAD_PATH, file_name)
                if os.path.exists(full_file_name):
                    os.remove(full_file_name)
                driver.find_element_by_xpath("//div[@id='table_top_bar']//ul/li[contains(text(),'导数据')]").click();
                time.sleep(task['sleep'])
                if os.path.exists(task['dest_file']):
                    os.remove(task['dest_file'])
                os.rename(full_file_name, task['dest_file'])

                cookie_update = {}
                for i in driver.get_cookies():
                    cookie_update[i["name"]] = i["value"]
                with open(Fundamental.WENCAI_COOKIE, "w") as f:
                    f.write(json.dumps(cookie_update))

                driver.close()
                time.sleep(3)

        pass

    # 日常处理下载
    @staticmethod
    def download_basic():
        engine = create_engine('sqlite:///' + Fundamental.FUNDA_DB)
        pro = ts.pro_api()
        df = pro.stock_basic()
        df.to_sql(Fundamental.BASIC_TB, engine, if_exists='replace', index=False)

    @staticmethod
    def get_codes():
        conn = sqlite3.connect(Fundamental.FUNDA_DB)
        cursor = conn.cursor()
        cursor.execute("SELECT symbol,name FROM '%s'" % Fundamental.BASIC_TB)
        result = {}
        for row in cursor:
            result[row[0]] = row[1]
        return result

    @staticmethod
    def remove_st(codes):
        all_codes = Fundamental.get_codes()
        return [code for code in codes if 'ST' not in all_codes[code]]

    @staticmethod
    def get_stocks():
        conn = sqlite3.connect(Fundamental.FUNDA_DB)
        cursor = conn.cursor()
        cursor.execute("SELECT symbol,name FROM '%s'" % Fundamental.BASIC_TB)
        return [row for row in cursor]

    @staticmethod
    #   业绩预告处理
    def yuzenjian():
        with open(Fundamental.YUZENJIAN) as f:
            table_name = Fundamental.YUZJ_TB
            root = lxml.etree.HTML(f.read())
            items = root.xpath('//tr')
            conn = sqlite3.connect(Fundamental.FUNDA_DB)
            cursor = conn.cursor()
            cursor.execute("drop table if exists '%s'" % table_name)
            cursor.execute(
                '''create table if not exists '%s' (
                                        code TEXT,
                                        name TEXT,
                                        type TEXT,
                                        pct real,
                                        prf real,
                                        prf_pre real,
                                        reason TEXT)'''
                % table_name)
            insert_sql = "insert into '%s' values (?,?,?,?,?,?,?)" % table_name

            for i in range(1, len(items)):
                tds = items[i].xpath('./td/text()')
                code = tds[0].strip()[0:-3]
                name = tds[1].strip()

                profit = tds[4].strip()
                type = tds[5].strip()
                profit_pre = tds[6].strip()
                profit_percent = tds[7].strip()
                reason = tds[9].strip()

                cursor.execute(insert_sql, (
                    code, name, type,
                    0 if profit_percent == '--' else float(profit_percent),
                    0 if profit == '--' else float(profit),
                    0 if profit_pre == '--' else float(profit_pre),
                    reason))
            conn.commit()

    @staticmethod
    #  增减持统计
    def zenjianchi():
        table_name = Fundamental.ZENJC_TB
        with open(Fundamental.ZENJIANCHI) as f:
            conn = sqlite3.connect(Fundamental.FUNDA_DB)
            cursor = conn.cursor()
            cursor.execute("drop table if exists '%s'" % table_name)
            cursor.execute(
                '''create table if not exists '%s' (
                code TEXT,
                name TEXT,
                dir TEXT,
                amount real,
                pct real,
                prg TEXT,
                f_dt TEXT,
                l_dt TEXT,
                s_dt TEXT,
                e_dt TEXT,
                holder TEXT
                )''' % table_name)
            insert_sql = "insert into '%s' values (?,?,?,?,?,?,?,?,?,?,?)" % table_name

            root = lxml.etree.HTML(f.read())
            items = root.xpath('//tr')
            for i in range(1, len(items)):
                tds = items[i].xpath('./td/text()')
                code = tds[0].strip()[0:-3]
                name = tds[1].strip()
                first_date = tds[4].strip()
                holder_type = tds[6].strip()
                dir = tds[7].strip()
                process = tds[8].strip()
                last_date = tds[9].strip()
                start_date = tds[10].strip()
                end_date = tds[11].strip()
                amount = tds[14].strip()
                percent = tds[15].strip()
                cursor.execute(insert_sql, (
                    code, name, dir,
                    0 if amount == '--' else float(amount),
                    0 if percent == '--' else float(percent),
                    process,
                    first_date, last_date,
                    start_date, end_date, holder_type))
            conn.commit()

    @staticmethod
    def export_txt():
        conn = sqlite3.connect(Fundamental.FUNDA_DB)
        cursor = conn.cursor()
        cursor.execute(
            "select distinct code from '%s' where type='扭亏' order by pct desc " % Fundamental.YUZJ_TB)
        u_write_to_file('/Users/hero101/Documents/t_niukui.txt', [row[0] for row in cursor])

        cursor.execute(
            "select distinct code from '%s' where type like '%%亏%%' and pct < 0 order by pct desc " % Fundamental.YUZJ_TB)
        u_write_to_file('/Users/hero101/Documents/t_kuisun.txt', [row[0] for row in cursor])

        cursor.execute(
            "select distinct code from '%s' where type='大幅上升' order by pct desc " % Fundamental.YUZJ_TB)
        u_write_to_file('/Users/hero101/Documents/t_dfss.txt', [row[0] for row in cursor])

        cursor.execute(
            "select distinct code from '%s' where type='预增' order by pct desc " % Fundamental.YUZJ_TB)
        u_write_to_file('/Users/hero101/Documents/t_yuzen.txt', [row[0] for row in cursor])

        cursor.execute("select distinct code from '%s' where type='预降' order by pct" % Fundamental.YUZJ_TB)
        u_write_to_file('/Users/hero101/Documents/t_yujian.txt', [row[0] for row in cursor])

        cursor.execute("select distinct code from '%s' where type='大幅下降' order by pct" % Fundamental.YUZJ_TB)
        u_write_to_file('/Users/hero101/Documents/t_dfxj.txt', [row[0] for row in cursor])

        cursor.execute(
            "select distinct code from '%s' where dir = '减持' and prg == '进行中' and pct > 0.1 and e_dt > '%s' and f_dt > '%s'" % (
                Fundamental.ZENJC_TB,
                u_month_befor(0),
                u_month_befor(3)))
        u_write_to_file('/Users/hero101/Documents/t_jianchi.txt', [row[0] for row in cursor])

        cursor.execute(
            "select distinct code from '%s' where dir = '增持' and prg == '进行中' and e_dt > '%s' and f_dt > '%s'" % (
                Fundamental.ZENJC_TB,
                u_month_befor(0),
                u_month_befor(3)))
        u_write_to_file('/Users/hero101/Documents/t_zenchi.txt', [row[0] for row in cursor])

    @staticmethod
    def _download_finance_process(dbname, stocks):
        if not stocks:
            return
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('blink-settings=imagesEnabled=false')
        driver = webdriver.Chrome(options=chrome_options)

        conn = sqlite3.connect(dbname)
        table_name = Fundamental.FINA_TB
        cursor = conn.cursor()
        cursor.execute("drop table if exists '%s'" % table_name)
        cursor.execute(
            '''create table if not exists '%s' (
            code TEXT,name TEXT,
            r_dt TEXT,np TEXT,npr TEXT,kfnp TEXT,kfnpr TEXT,rv TEXT,rvr TEXT)''' % table_name)
        insert_sql = "insert into '%s' values (?,?,?,?,?,?,?,?,?)" % table_name
        count = 0
        for stock in stocks:
            print(count)
            count += 1
            # 'http://stockpage.10jqka.com.cn/%s/finance/'
            fina_url = 'http://basic.10jqka.com.cn/%s/finance.html' % stock[0]
            # fina_url = 'http://stockpage.10jqka.com.cn/%s/finance/' % stock[0]
            driver.get(fina_url)
            # driver.switch_to.frame('dataifm')
            root = lxml.etree.HTML(driver.page_source)
            report_date = root.xpath('//div[@class="data_tbody"]//table[@class="top_thead"]//th/div/text()')
            items = root.xpath('//div[@class="data_tbody"]//table[@class="tbody"]//tr')
            if items:
                net_profit = items[1].xpath('./td/text()')
                net_profit_rate = items[2].xpath('./td/text()')
                kf_net_profit = items[3].xpath('./td/text()')
                kf_net_profit_rate = items[4].xpath('./td/text()')
                revenue = items[5].xpath('./td/text()')
                revenue_rate = items[6].xpath('./td/text()')

                length = min(len(net_profit), len(report_date), 21)
                for i in range(length):
                    cursor.execute(insert_sql, (
                        stock[0], stock[1], report_date[i], net_profit[i], net_profit_rate[i], kf_net_profit[i],
                        kf_net_profit_rate[i], revenue[i], revenue_rate[i]))
            # break
        conn.commit()
        driver.close()

    @staticmethod
    def download_finance_data():
        all_stocks = Fundamental.get_stocks()
        process_count = 9

        process_per = int(len(all_stocks) / process_count + 3)
        # p = Pool(process_count)
        dbs = []
        pp = []
        for index in range(process_count):
            db_name = 'data/fina_%s.db' % index
            dbs.append(db_name)
            stocks = all_stocks[index * process_per:(index + 1) * process_per]
            p = Process(target=Fundamental._download_finance_process, args=(db_name, stocks,))
            p.start()
            pp.append(p)
            # break

        for p in pp:
            p.join()

        conn = sqlite3.connect(Fundamental.FUNDA_DB)
        table_name = Fundamental.FINA_TB
        cursor = conn.cursor()
        cursor.execute("drop table if exists '%s'" % table_name)
        cursor.execute(
            '''create table if not exists '%s' (
            code TEXT,name TEXT,
            r_dt TEXT,np TEXT,npr TEXT,kfnp TEXT,kfnpr TEXT,rv TEXT,rvr TEXT)''' % table_name)
        insert_sql = "insert into '%s' values (?,?,?,?,?,?,?,?,?)" % table_name

        for db in dbs:
            if os.path.exists(db):
                conn_src = sqlite3.connect(db)
                cursor_src = conn_src.cursor()
                cursor_src.execute("select * from '%s'" % table_name)
                for row in cursor_src:
                    cursor.execute(insert_sql, row)
                conn_src.close()
                os.remove(db)
            else:
                print(db, ' not exist!!')

        conn.commit()
        cursor.execute("create index if not exists 'fina_code_date' on '%s'(code,r_dt)" % table_name)
        cursor.execute(
            "delete from '%s' where np = '--' and npr = '--' and kfnp = '--' and rv = '--' and rvr = '--'" % table_name)
        conn.commit()

    @staticmethod
    def analyze_finance_data():
        conn = sqlite3.connect(Fundamental.FUNDA_DB)
        fina_table_name = Fundamental.FINA_TB
        cursor = conn.cursor()
        cursor.execute("select distinct code from '%s'" % fina_table_name)
        codes = [row[0] for row in cursor]

        analyze_table_name = Fundamental.ANAL_FINA_TB
        cursor.execute("drop table if exists '%s'" % analyze_table_name)
        cursor.execute(
            '''create table if not exists '%s' (
                    code TEXT,
                    name TEXT,
                    lrd TEXT,
                    lrvr real,
                    lnpr real,
                    lkfnpr real,                
                    rv real,
                    np real,
                    kfnp real,
                    rvr1 real,
                    rvr2 real,
                    rvr3 real,
                    rvr4 real,
                    npr1 real,
                    npr2 real,
                    npr3 real,
                    npr4 real,
                    kfnpr1 real,
                    kfnpr2 real,
                    kfnpr3 real,
                    kfnpr4 real,
                    q1rv TEXT,
                    q1np TEXT,
                    q1kfnp TEXT,
                    q2rv TEXT,
                    q2np TEXT,
                    q2kfnp TEXT,
                    q3rv TEXT,
                    q3np TEXT,
                    q3kfnp TEXT )''' % analyze_table_name)
        insert_sql = "insert into '%s' values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" % analyze_table_name

        for code in codes:
            cursor.execute("select * from '%s' where code = '%s' order by r_dt desc" % (fina_table_name, code))
            # 最后报告期
            lrd = ''
            # 最后报告期 营收同比增长率
            lrvr = 0
            # 最后报告期 净利润同比增长率
            lnpr = 0
            # 最后报告期 扣非净利润同比增长率
            lkfnpr = 0
            name = ''
            # 最近一年的营收、净利润、扣非净利润
            rv = 0
            np = 0
            kfnp = 0
            # 最近5年的营收增长率
            rvr1 = 0
            rvr2 = 0
            rvr3 = 0
            rvr4 = 0
            # 最近5年的利润增长率
            npr1 = 0
            npr2 = 0
            npr3 = 0
            npr4 = 0
            # 最近5年的扣非利润增长率
            kfnpr1 = 0
            kfnpr2 = 0
            kfnpr3 = 0
            kfnpr4 = 0
            quater1 = {}
            quater2 = {}
            quater3 = {}
            quater4 = {}
            first_row = True
            for row in cursor:
                if first_row:
                    lrd = row[2]
                    name = row[1]
                    lnpr = Fundamental.rate_text_to_float(row[4])
                    lkfnpr = Fundamental.rate_text_to_float(row[6])
                    lrvr = Fundamental.rate_text_to_float(row[8])
                    first_row = False
                year = row[2][:4]
                q_date = row[2][-5:]
                if q_date == '03-31':
                    quater1[year] = (row[3], row[4], row[5], row[6], row[7], row[8])
                elif q_date == '06-30':
                    quater2[year] = (row[3], row[4], row[5], row[6], row[7], row[8])
                elif q_date == '09-30':
                    quater3[year] = (row[3], row[4], row[5], row[6], row[7], row[8])
                elif q_date == '12-31':
                    quater4[year] = (row[3], row[4], row[5], row[6], row[7], row[8])
                    lenq4 = len(quater4)
                    if lenq4 == 1:
                        rv = Fundamental.amount_text_to_float(row[7])
                        np = Fundamental.amount_text_to_float(row[3])
                        kfnp = Fundamental.amount_text_to_float(row[5])
                        rvr1 = Fundamental.rate_text_to_float(row[8])
                        npr1 = Fundamental.rate_text_to_float(row[4])
                        kfnpr1 = Fundamental.rate_text_to_float(row[6])
                    elif lenq4 == 2:
                        rvr2 = Fundamental.rate_text_to_float(row[8])
                        npr2 = Fundamental.rate_text_to_float(row[4])
                        kfnpr2 = Fundamental.rate_text_to_float(row[6])
                    elif lenq4 == 3:
                        rvr3 = Fundamental.rate_text_to_float(row[8])
                        npr3 = Fundamental.rate_text_to_float(row[4])
                        kfnpr3 = Fundamental.rate_text_to_float(row[6])
                    elif lenq4 == 4:
                        rvr4 = Fundamental.rate_text_to_float(row[8])
                        npr4 = Fundamental.rate_text_to_float(row[4])
                        kfnpr4 = Fundamental.rate_text_to_float(row[6])

            now_year = datetime.date.today().year
            # 只计算5年数据

            # 1季度 营收、利润、扣非利润最近5年走势
            q1rv = []
            q1np = []
            q1kfnp = []
            # 2季度 营收、利润、扣非利润走势
            q2rv = []
            q2np = []
            q2kfnp = []
            # 3季度 营收、利润、扣非利润走势
            q3rv = []
            q3np = []
            q3kfnp = []

            for year in range(now_year, now_year - 5, -1):
                year_text = '%s' % year
                if year_text in quater1:
                    q1rv.append(str(Fundamental.rate_text_to_float(quater1[year_text][5])))
                    q1np.append(str(Fundamental.amount_text_to_float(quater1[year_text][0])))
                    q1kfnp.append(str(Fundamental.amount_text_to_float(quater1[year_text][2])))
                else:
                    q1rv.append('0')
                    q1np.append('0')
                    q1kfnp.append('0')

                if year_text in quater2:
                    q2rv.append(str(Fundamental.rate_text_to_float(quater2[year_text][5])))
                    q2np.append(str(Fundamental.amount_text_to_float(quater2[year_text][0])))
                    q2kfnp.append(str(Fundamental.amount_text_to_float(quater2[year_text][2])))
                else:
                    q2rv.append('0')
                    q2np.append('0')
                    q2kfnp.append('0')

                if year_text in quater3:
                    q3rv.append(str(Fundamental.rate_text_to_float(quater3[year_text][5])))
                    q3np.append(str(Fundamental.amount_text_to_float(quater3[year_text][0])))
                    q3kfnp.append(str(Fundamental.amount_text_to_float(quater3[year_text][2])))
                else:
                    q3rv.append('0')
                    q3np.append('0')
                    q3kfnp.append('0')
                pass

            cursor.execute(insert_sql, (
                code, name, lrd, lrvr, lnpr, lkfnpr, rv, np, kfnp, rvr1, rvr2, rvr3, rvr4, npr1, npr2, npr3, npr4,
                kfnpr1,
                kfnpr2, kfnpr3, kfnpr4, '--'.join(q1rv), '--'.join(q1np), '--'.join(q1kfnp), '--'.join(q2rv),
                '--'.join(q2np),
                '--'.join(q2kfnp), '--'.join(q3rv), '--'.join(q3np), '--'.join(q3kfnp)))

        conn.commit()

    pass

    @staticmethod
    def download_industry():
        all_stocks = Fundamental.get_stocks()
        conn = sqlite3.connect(Fundamental.FUNDA_DB)
        table_name = Fundamental.INDUSTRY_TB
        cursor = conn.cursor()
        # cursor.execute("drop table if exists '%s'" % table_name)
        cursor.execute(
            '''create table if not exists '%s' (
                                        code TEXT,
                                        name TEXT,
                                        field1 TEXT,
                                        field2 TEXT,
                                        field3 TEXT)''' % table_name)
        cursor.execute("select distinct code from '%s'" % table_name)
        exist_codes = {row[0] for row in cursor}
        insert_sql = "insert into '%s' values (?,?,?,?,?)" % table_name
        req_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 5.1; rv:5.0) Gecko/20100101 Firefox/5.0',
        }
        count = 0
        for stock in all_stocks:
            print(count)
            count += 1
            if stock[0] in exist_codes:
                continue
            field_url = 'http://basic.10jqka.com.cn/%s/field.html' % stock[0]
            field_req = urllib.request.Request(field_url, headers=req_headers)
            field_resp = urllib.request.urlopen(field_req).read()
            field_root = lxml.etree.HTML(field_resp)
            fields_text = field_root.xpath('//span[@class="tip f14"]/text()')[0]
            fields = [e.strip() for e in re.split('[-（]', fields_text) if e][0:3]
            cursor.execute(insert_sql, (
                stock[0], stock[1], fields[0], fields[1], fields[2]))
            conn.commit()
            # break
        pass

    hot_concepts = ['5G', '华为', '芯片', 'OLED', '高送转', '半导体', '区块链', '黄金', '军工', '稀土', '集成电路', '人工智能']

    @staticmethod
    def get_codes_by_concept(concepts):
        conn = sqlite3.connect(Fundamental.FUNDA_DB)
        cursor = conn.cursor()
        table_name = Fundamental.CONCEPT_TB
        concepts_cond = ["concept like '%%%s%%'" % e for e in concepts]
        sql = "select distinct code from '%s' " % (table_name,)
        if concepts_cond:
            sql = "select distinct code from '%s' where %s" % (table_name, ' or '.join(concepts_cond))
        cursor.execute(sql)
        return [row[0] for row in cursor]

    @staticmethod
    def download_concept():
        all_stocks = Fundamental.get_stocks()
        conn = sqlite3.connect(Fundamental.FUNDA_DB)
        table_name = Fundamental.CONCEPT_TB
        cursor = conn.cursor()
        # cursor.execute("drop table if exists '%s'" % table_name)
        cursor.execute(
            '''create table if not exists '%s' (
                                        code TEXT,
                                        name TEXT,
                                        concept TEXT,
                                        concept_e TEXT,
                                        concept_o TEXT)''' % table_name)
        cursor.execute("select distinct code from '%s'" % table_name)
        exist_codes = {row[0] for row in cursor}
        insert_sql = "insert into '%s' values (?,?,?,?,?)" % table_name
        req_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 5.1; rv:5.0) Gecko/20100101 Firefox/5.0',
        }
        count = 0
        for stock in all_stocks:
            print(count)
            count += 1
            if stock[0] in exist_codes:
                continue
            concept_url = 'http://basic.10jqka.com.cn/%s/concept.html' % stock[0]
            concept_req = urllib.request.Request(concept_url, headers=req_headers)
            concept_resp = urllib.request.urlopen(concept_req).read()
            concept_root = lxml.etree.HTML(concept_resp)

            normal = concept_root.xpath("//div[@id='concept']//tbody/tr")
            n_c = []
            for i in range(int(len(normal) / 2)):
                n_c.append(normal[i * 2].xpath('./td/text()')[1].strip())
            emerging = concept_root.xpath("//div[@id='emerging']//tbody/tr")
            n_e = []
            for i in range(int(len(emerging) / 2)):
                n_e.append(emerging[i * 2].xpath('./td/text()')[1].strip())

            other = concept_root.xpath("//div[@id='other']//tbody/tr")
            n_o = []
            for i in range(int(len(other) / 2)):
                n_o.append(other[i * 2].xpath('./td/text()')[1].strip())
            cursor.execute(insert_sql, (
                stock[0], stock[1], ','.join(n_c), ','.join(n_e), ','.join(n_o)))
            conn.commit()
            # break
        pass

    @staticmethod
    def daily_run():
        Fundamental.download_basic()
        Fundamental.download_from_wencai()
        Fundamental.zenjianchi()
        Fundamental.yuzenjian()
        Fundamental.export_txt()

        pass

    @staticmethod
    def get_fundamental(code):

        conn = sqlite3.connect(Fundamental.FUNDA_DB)
        table_name = Fundamental.INDUSTRY_TB
        cursor = conn.cursor()

        cursor.execute("select name,list_date,area from '%s' where symbol='%s'" % (Fundamental.BASIC_TB, code))
        result = [row for row in cursor]
        if not result:
            return
        name, list_date, area = result[0]

        cursor.execute("select field1,field2,field3 from '%s' where code='%s'" % (Fundamental.INDUSTRY_TB, code))
        result = [row for row in cursor]
        fld1, fld2, fld3 = result[0] if result else ('','','')

        cursor.execute("select concept,concept_e,concept_o from '%s' where code='%s'" % (Fundamental.CONCEPT_TB, code))
        concept = [c for c in [row for row in cursor][0] if c]

        cursor.execute("select type,pct from '%s' where code='%s'" % (Fundamental.YUZJ_TB, code))
        yuzenjian = [row for row in cursor]
        yzj_info = "业绩预告:    暂无"
        if yuzenjian:
            yzj_info = "业绩预告:    %s   %s%%" % yuzenjian[0]

        cursor.execute(
            "select f_dt, dir,amount,pct,holder from '%s' where code='%s' and f_dt>'%s' order by f_dt desc " % (
                Fundamental.ZENJC_TB, code, u_month_befor(6)))
        zenjianchi = [row for row in cursor]
        zjc_info = "最近6个月增减持计划：    无"
        if zenjianchi:
            zjc_info = "最近6个月增减持计划:"
            for item in zenjianchi:
                item_info = '\n  %s 初次公告 %s计划   %10.1f股  占比%4.2f%%  %s' % item
                zjc_info += item_info

        cursor.execute(''' select lrvr,lnpr,lkfnpr,
                            rv,np,kfnp,rvr1,rvr2,rvr3,rvr4,
                            npr1,npr2,npr3,npr4,
                            kfnpr1,kfnpr2,kfnpr3,kfnpr4
                            from '%s' where code='%s' ''' % (Fundamental.ANAL_FINA_TB, code))

        result = [row for row in cursor]
        if not result:
            return ''
        (lrvr, lnpr, lkfnpr, rv, np, kfnp,
         rvr1, rvr2, rvr3, rvr4,
         npr1, npr2, npr3, npr4,
         kfnpr1, kfnpr2, kfnpr3, kfnpr4) = result[0]

        info = '''%s(%s)   所在地:%s  上市日期:%s   %s-%s-%s  
概念题材:   %s
%s
最近报告期业绩:  营收同比%s%%  净利润同比%s%%  扣非净利润同比%s%%
年度业绩:  营收          %8.5f亿     历年同比 %s%%   %s%%   %s%%   %s%%
          净利润        %8.5f亿     历年同比 %s%%   %s%%   %s%%   %s%%
          扣非净利润     %8.5f亿     历年同比 %s%%   %s%%   %s%%   %s%%
%s''' % (
            name, code, area, list_date, fld1, fld2, fld3, ','.join(concept),
            yzj_info,
            lrvr, lnpr, lkfnpr,
            rv, rvr4, rvr3, rvr2, rvr1,
            np, npr4, npr3, npr2, npr1,
            kfnp, kfnpr4, kfnpr3, kfnpr2, kfnpr1,
            zjc_info
        )
        return info
        # print(info)


if __name__ == '__main__':
    import cProfile

    # Fundamental.download_from_wencai()
    # Fundamental.yuzenjian()
    # Fundamental.zenjianchi()
    # Fundamental.export_txt()
    # Fundamental.download_basic()
    # Fundamental.download_industry()
    # Fundamental.download_concept()
    # Fundamental.download_finance_data()
    # Fundamental.analyze_finance_data()

    # cProfile.run('Fundamental.download_finance_data()')
    # cProfile.run('Fundamental.download_industry_concept()')
    #

    # cProfile.run('Fundamental.download_industry()')
    # Fundamental.download_concept()
    # cProfile.run('Fundamental.daily_run()')
    # Fundamental.get_fundamental('300134')
    Fundamental.create_wencai_cookie()
    pass
