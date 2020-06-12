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


# brew cask reinstall chromedriver

# xattr -d com.apple.quarantine chromedriver


# 数据处理
class Fundamental(object):
    FUNDA_DB = 'data/fundamental.db'
    WENCAI_COOKIE = 'data/wencai_cookie.txt'
    WENCAI_HOME_URL = 'http://www.iwencai.com/?allow_redirect=false'
    DOWNLOAD_PATH = '/Users/hero101/Downloads'
    ZENJIANCHI = 'data/zenjianchi.xml'
    YUZENJIAN = 'data/yuzenjian.xml'
    JIEJINNEXT = 'data/jiejinnext.xml'
    JIEJINCUR = 'data/jiejincur.xml'
    JIEJINPRE = 'data/jiejinpre.xml'
    YUZJ_TB = 'yuzenjian'
    JIEJIN_TB = 'jiejin'
    ZENJC_TB = 'zenjianchi'
    FINA_TB = 'finance'
    BASIC_TB = 'basic'
    ANAL_FINA_TB = 'analyze_finance'
    INDUSTRY_TB = 'industry'
    OPERATE_TB = 'operate'
    DOCTOR_TB = 'doctor'
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
        # chrome_options.add_argument('blink-settings=imagesEnabled=false')
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
                'sleep': 8
            },
            {
                'query_url': 'http://www.iwencai.com/stockpick/search?typed=1&preParams=&ts=1&f=3&qs=result_rewrite&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w=最近8个月增减持计划&queryarea=',
                'dest_file': Fundamental.ZENJIANCHI,
                'sleep': 8
            },
            {
                'query_url': 'http://www.iwencai.com/stockpick/search?typed=1&preParams=&ts=1&f=1&qs=result_rewrite&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w=下个月解禁&queryarea=',
                'dest_file': Fundamental.JIEJINNEXT,
                'sleep': 8
            },
            {
                'query_url': 'http://www.iwencai.com/stockpick/search?typed=1&preParams=&ts=1&f=1&qs=result_rewrite&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w=当月解禁&queryarea=',
                'dest_file': Fundamental.JIEJINCUR,
                'sleep': 8
            },
            {
                'query_url': 'http://www.iwencai.com/stockpick/search?typed=1&preParams=&ts=1&f=1&qs=result_rewrite&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w=上个月解禁&queryarea=',
                'dest_file': Fundamental.JIEJINPRE,
                'sleep': 8
            },

        ]
        with open(Fundamental.WENCAI_COOKIE) as f:
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium import webdriver
            cookie = json.load(f)
            chrome_options = Options()
            # chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('blink-settings=imagesEnabled=false')

            for task in download_tasks:
                driver = webdriver.Chrome(options=chrome_options)
                driver.get(Fundamental.WENCAI_HOME_URL)
                for key in cookie:
                    print(key, cookie[key])
                    if key in ['v', 'PHPSESSID', 'guidState']:
                        continue
                    driver.add_cookie({'name': key, 'value': cookie[key]})
                driver.get(task['query_url'])
                file_name = datetime.date.today().strftime('%Y-%m-%d.xls')
                full_file_name = '%s/%s' % (Fundamental.DOWNLOAD_PATH, file_name)
                if os.path.exists(full_file_name):
                    os.remove(full_file_name)
                time.sleep(1)
                try:
                    tip = driver.find_element_by_xpath(
                        "//div[@class='popup_win hidden popup_tip_orange thead_drag_tip popup_tip']//span")
                    tip.click()
                except:
                    pass
                time.sleep(1)
                f = driver.find_element_by_xpath("//div[@id='table_top_bar']//ul/li[contains(text(),'导数据')]")
                f.click();
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
        return [code for code in codes if ((code in all_codes) and ('ST' not in all_codes[code]))]

    @staticmethod
    def get_stocks():
        conn = sqlite3.connect(Fundamental.FUNDA_DB)
        cursor = conn.cursor()
        cursor.execute("SELECT symbol,name FROM '%s'" % Fundamental.BASIC_TB)
        return [row for row in cursor]

    @staticmethod
    def get_names():
        conn = sqlite3.connect(Fundamental.FUNDA_DB)
        cursor = conn.cursor()
        cursor.execute("SELECT symbol,name FROM '%s'" % Fundamental.BASIC_TB)
        return {row[1]: row[0] for row in cursor}

    @staticmethod
    #   解禁信息处理
    def jiejin():
        table_name = Fundamental.JIEJIN_TB
        conn = sqlite3.connect(Fundamental.FUNDA_DB)
        cursor = conn.cursor()
        cursor.execute("drop table if exists '%s'" % table_name)
        cursor.execute(
            '''create table if not exists '%s' (
                                    code TEXT,
                                    name TEXT,
                                    date_ TEXT,
                                    counts real,
                                    pct real,
                                    amount real,
                                    type_ TEXT,
                                    cost TEXT)'''
            % table_name)
        conn.close()

        def _jiejin_fu(file):
            # Fundamental.JIEJINPRE
            with open(file) as f:
                table_name = Fundamental.JIEJIN_TB
                conn = sqlite3.connect(Fundamental.FUNDA_DB)
                cursor = conn.cursor()
                insert_sql = "insert into '%s' values (?,?,?,?,?,?,?,?)" % table_name
                root = lxml.etree.HTML(f.read())
                items = root.xpath('//tr')

                for i in range(1, len(items)):
                    tds = items[i].xpath('./td/text()')
                    code = tds[0].strip()[0:-3]
                    name = tds[1].strip()
                    date_ = tds[4].strip()
                    counts = tds[5].strip()
                    pct = tds[6].strip()
                    amount = tds[7].strip()
                    type_ = tds[8].strip()
                    cost = tds[10].strip()
                    cursor.execute(insert_sql, (
                        code, name, date_, counts, pct, amount, type_, cost))
                conn.commit()

        _jiejin_fu(Fundamental.JIEJINPRE)
        _jiejin_fu(Fundamental.JIEJINCUR)
        _jiejin_fu(Fundamental.JIEJINNEXT)

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
        u_write_to_file(u_create_path_by_system('t_niukui.txt'), [row[0] for row in cursor])

        cursor.execute(
            "select distinct code from '%s' where type like '%%亏%%' and pct < 0 order by pct desc " % Fundamental.YUZJ_TB)
        u_write_to_file(u_create_path_by_system('t_kuisun.txt'), [row[0] for row in cursor])

        cursor.execute(
            "select distinct code from '%s' where type='大幅上升' order by pct desc " % Fundamental.YUZJ_TB)
        u_write_to_file(u_create_path_by_system('t_dfss.txt'), [row[0] for row in cursor])

        cursor.execute(
            "select distinct code from '%s' where type='预增' order by pct desc " % Fundamental.YUZJ_TB)
        u_write_to_file(u_create_path_by_system('t_yuzen.txt'), [row[0] for row in cursor])

        cursor.execute("select distinct code from '%s' where type='预降' order by pct" % Fundamental.YUZJ_TB)
        u_write_to_file(u_create_path_by_system('t_yujian.txt'), [row[0] for row in cursor])

        cursor.execute("select distinct code from '%s' where type='大幅下降' order by pct" % Fundamental.YUZJ_TB)
        u_write_to_file(u_create_path_by_system('t_dfxj.txt'), [row[0] for row in cursor])

        cursor.execute(
            "select distinct code from '%s' where dir = '减持' and prg == '进行中' and pct > 0.1 and e_dt > '%s' and f_dt > '%s'" % (
                Fundamental.ZENJC_TB,
                u_month_befor(0),
                u_month_befor(6)))
        u_write_to_file(u_create_path_by_system('t_jianchi.txt'), [row[0] for row in cursor])

        cursor.execute(
            "select distinct code from '%s' where dir = '增持' and prg == '进行中' and e_dt > '%s' and f_dt > '%s'" % (
                Fundamental.ZENJC_TB,
                u_month_befor(0),
                u_month_befor(6)))
        u_write_to_file(u_create_path_by_system('t_zenchi.txt'), [row[0] for row in cursor])

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
                net_profit = items[1].xpath('./td/div/text()')
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
        process_count = 7

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
            fields_text = field_root.xpath('//span[@class="tip f14"]/text()')
            if fields_text:
                fields_text = fields_text[0]
            else:
                continue
            fields = [e.strip() for e in re.split('[-（]', fields_text) if e][0:3]
            cursor.execute(insert_sql, (
                stock[0], stock[1], fields[0], fields[1], fields[2]))
            conn.commit()
            # break
        pass

    @staticmethod
    def download_operate():
        all_stocks = Fundamental.get_stocks()
        conn = sqlite3.connect(Fundamental.FUNDA_DB)
        table_name = Fundamental.OPERATE_TB
        cursor = conn.cursor()
        # cursor.execute("drop table if exists '%s'" % table_name)
        cursor.execute(
            '''create table if not exists '%s' (
                                        code TEXT,
                                        name TEXT,
                                        operate TEXT)''' % table_name)
        cursor.execute("select distinct code from '%s'" % table_name)
        exist_codes = {row[0] for row in cursor}
        insert_sql = "insert into '%s' values (?,?,?)" % table_name
        req_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 5.1; rv:5.0) Gecko/20100101 Firefox/5.0',
        }
        count = 0
        for stock in all_stocks:
            print(count)
            count += 1
            if stock[0] in exist_codes:
                continue
            operate_url = 'http://basic.10jqka.com.cn/%s/operate.html' % stock[0]
            operate_req = urllib.request.Request(operate_url, headers=req_headers)
            operate_resp = urllib.request.urlopen(operate_req).read()
            operate_root = lxml.etree.HTML(operate_resp)

            operate = operate_root.xpath('//div[@class="mt15"]//li/p/text()')
            if operate:
                operate = operate[0]
            else:
                operate = ''
            # print(stock[0], stock[1], operate)
            cursor.execute(insert_sql, (
                stock[0], stock[1], operate))
            conn.commit()
            # break
        pass

    @staticmethod
    def get_codes_by_concept(concepts):
        conn = sqlite3.connect(Fundamental.FUNDA_DB)
        cursor = conn.cursor()
        table_name = Fundamental.CONCEPT_TB
        concepts_cond = ["concept like '%%%s%%'" % e for e in concepts]
        concepts_cond += ["concept_e like '%%%s%%'" % e for e in concepts]
        concepts_cond += ["concept_o like '%%%s%%'" % e for e in concepts]
        sql = "select distinct code from '%s' " % (table_name,)
        if concepts_cond:
            sql = "select distinct code from '%s' where %s" % (table_name, ' or '.join(concepts_cond))
        cursor.execute(sql)
        return [row[0] for row in cursor]

    @staticmethod
    def pick_codes_by_concept(concepts):
        from synthesis import synthesis
        conn = sqlite3.connect(Fundamental.FUNDA_DB)
        cursor = conn.cursor()
        table_name = Fundamental.CONCEPT_TB
        concepts_cond = ["(concept like '%%%s%%' or concept_e like '%%%s%%' or concept_o like '%%%s%%')" % (e, e, e) for
                         e in concepts]
        sql = "select distinct code from '%s' " % (table_name,)
        if concepts_cond:
            sql = "select distinct code from '%s' where %s" % (table_name, ' and '.join(concepts_cond))
        cursor.execute(sql)
        codes = [row[0] for row in cursor]
        u_write_to_file('data/pick_concepts.txt', codes)
        u_write_to_file(u_create_path_by_system('t_pick_concepts.txt') , codes)
        synthesis('data/pick_concepts.txt')
        # return [row[0] for row in cursor]

    @staticmethod
    def name_to_codes(targets):
        names = Fundamental.get_names()
        result = []
        for target in targets:
            if len(target) != 6:
                if target in names:
                    result.append(names[target])
            else:
                result.append(target)
        return result

    @staticmethod
    def update_bellwether():

        conn = sqlite3.connect(Fundamental.FUNDA_DB)
        table_name = Fundamental.CONCEPT_TB
        cursor = conn.cursor()
        cursor.execute("select code,concept from %s" % table_name)
        concepts = {row[0]: row[1].split(',') for row in cursor}
        cpts = set()
        codes = []
        for code in concepts:
            cs = set(concepts[code])
            if cs - cpts:
                codes.append(code)
                cpts |= cs
        bellwether = {}
        req_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 5.1; rv:5.0) Gecko/20100101 Firefox/5.0',
        }
        count = 0
        for stock in codes:
            print(count)
            count += 1
            concept_url = 'http://basic.10jqka.com.cn/%s/concept.html' % stock
            concept_req = urllib.request.Request(concept_url, headers=req_headers)
            concept_resp = urllib.request.urlopen(concept_req).read()
            concept_root = lxml.etree.HTML(concept_resp)

            normal = concept_root.xpath("//div[@id='concept']//tbody/tr")
            for i in range(int(len(normal) / 2)):
                c = normal[i * 2].xpath('./td/text()')[1].strip()
                bw = normal[i * 2].xpath('./td/a/text()')
                bellwether[c] = bw

        hot_concepts = ','.join(u_read_input('data/hot_concepts'))
        allbw = set()
        hotbw = set()
        for c in bellwether:
            allbw |= set(bellwether[c])
            if c in hot_concepts:
                hotbw |= set(bellwether[c])
        u_write_to_file(u_create_path_by_system('t_bw_all.txt'), Fundamental.name_to_codes(allbw))
        u_write_to_file(u_create_path_by_system('t_bw_hot.txt'), Fundamental.name_to_codes(hotbw))
        pass

    @staticmethod
    def download_concept():
        all_stocks = Fundamental.get_stocks()
        conn = sqlite3.connect(Fundamental.FUNDA_DB)
        table_name = Fundamental.CONCEPT_TB
        cursor = conn.cursor()
        # cursor.execute("drop table if exists '%s'" % table_name)
        bellwether = {}
        cursor.execute(
            '''create table if not exists '%s' (
                                        code TEXT,
                                        name TEXT,
                                        concept TEXT,
                                        concept_e TEXT,
                                        concept_o TEXT,
                                        concept_jx TEXT,
                                        concept_ejx TEXT,
                                        concept_ojx TEXT
                                        )''' % table_name)
        cursor.execute("select distinct code from '%s'" % table_name)
        exist_codes = {row[0] for row in cursor}
        insert_sql = "insert into '%s' values (?,?,?,?,?,?,?,?)" % table_name
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
            n_cjx = []
            for i in range(int(len(normal) / 2)):
                c = normal[i * 2].xpath('./td/text()')[1].strip()
                cjx = normal[i * 2 + 1].xpath('./td/div/text()')
                if len(cjx) >= 2:
                    cjx = cjx[1].strip()
                else:
                    cjx = "暂无概念解析"
                n_c.append(c)
                n_cjx.append(cjx)
                bw = normal[i * 2].xpath('./td/a/text()')
                bellwether[c] = bw
                # print(cjx)
                # print(c)
                # print(bw)
            emerging = concept_root.xpath("//div[@id='emerging']//tbody/tr")
            n_e = []
            n_ejx = []
            for i in range(int(len(emerging) / 2)):
                n_e.append(emerging[i * 2].xpath('./td/text()')[1].strip())
                ejx = emerging[i * 2 + 1].xpath('./td/div/text()')
                if len(ejx) >= 2:
                    ejx = ejx[1].strip()
                else:
                    ejx = "暂无概念解析"
                n_ejx.append(ejx)

            other = concept_root.xpath("//div[@id='other']//tbody/tr")
            n_o = []
            n_ojx = []
            for i in range(int(len(other) / 2)):
                n_o.append(other[i * 2].xpath('./td/text()')[1].strip())
                ojx = other[i * 2 + 1].xpath('./td/div/text()')
                if len(ojx) >= 2:
                    ojx = ojx[1].strip()
                else:
                    ojx = "暂无概念解析"
                n_ojx.append(ojx)

            cursor.execute(insert_sql, (
                stock[0], stock[1], ','.join(n_c), ','.join(n_e), ','.join(n_o),
                '|||'.join(n_cjx), '|||'.join(n_ejx), '|||'.join(n_ojx),))
            conn.commit()
            # break

        hot_concepts = ','.join(u_read_input('data/hot_concepts'))
        allbw = set()
        hotbw = set()
        for c in bellwether:
            allbw |= set(bellwether[c])
            if c in hot_concepts:
                hotbw |= set(bellwether[c])
        u_write_to_file(u_create_path_by_system('t_bw_all.txt'), Fundamental.name_to_codes(allbw))
        u_write_to_file(u_create_path_by_system('t_bw_hot.txt'), Fundamental.name_to_codes(hotbw))

        # u_write_to_file('/Users/hero101/Documents/t_bellwether.txt',
        #                 ['%s:%s' % (k, ','.join(bellwether[k])) for k in bellwether])
        pass

    @staticmethod
    def daily_run():
        Fundamental.download_basic()
        Fundamental.download_from_wencai()
        Fundamental.zenjianchi()
        Fundamental.yuzenjian()
        Fundamental.jiejin()
        Fundamental.export_txt()

        pass

    @staticmethod
    def download_doctor():
        all_stocks = Fundamental.get_stocks()
        conn = sqlite3.connect(Fundamental.FUNDA_DB)
        table_name = Fundamental.DOCTOR_TB
        cursor = conn.cursor()
        # cursor.execute("drop table if exists '%s'" % table_name)
        cursor.execute(
            '''create table if not exists '%s' (
                                        code TEXT,
                                        name TEXT,
                                        cpr INT,
                                        tcr INT,
                                        fdr INT,
                                        mgr INT,
                                        inr INT,
                                        bsr INT,
                                        cpl TEXT,
                                        cyw TEXT,
                                        minc INT,
                                        minr FLOAT,
                                        date TEXT)''' % table_name)

        cursor.execute("select distinct code from '%s'" % table_name)
        exist_codes = {row[0] for row in cursor}
        insert_sql = "insert into '%s' values (?,?,?,?,?,?,?,?,?,?,?,?,?)" % table_name

        req_headers = {
            "User-Agent":
                "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36"
        }
        count = 0
        today = u_day_befor(0)
        for stock in all_stocks:
            print(count, stock)
            count += 1

            if stock[0] in exist_codes:
                continue

            # if count % 100 == 0:
            #     time.sleep(60)
            #
            # if count % 2000 == 0:
            #     time.sleep(600)

            cpr = 50  # 综合排名比
            cpl = ''  # 综合评级
            tcr = 50  # 技术排名比
            fdr = 50  # 资金排名比
            mgr = 50  # 消息排名比
            inr = 50  # 行业排名比
            bsr = 50  # 基本面排名比
            # rstp = 0.0  # 阻力价格
            # sptp = 0.0  # 支撑价格
            # avgp = 0.0  # 平均成本价格
            cyw = ''  # 主力控盘程度
            minc = 0  # 主力机构家数
            minr = 0.0  # 主力机构持股占比

            doctor_url = 'http://doctor.10jqka.com.cn/%s/' % stock[0]
            # proxy = u_get_proxy().get("proxy")
            # proxy_handler = urllib.request.ProxyHandler({"http": proxy})
            # opener = urllib.request.build_opener(proxy_handler)
            doctor_req = urllib.request.Request(doctor_url, headers=req_headers)
            doctor_resp = urllib.request.urlopen(doctor_req, timeout=100).read()
            doctor_root = lxml.etree.HTML(doctor_resp)
            cpr_text = doctor_root.xpath('//div[@class="stocktotal"]/text()')
            if not cpr_text:
                continue
            else:
                cpr_text = cpr_text[0]
            cpr = int(re.findall(r"打败了(.+?)%的股票", cpr_text)[0])
            cpl = doctor_root.xpath('//div[@class="value_bar"]//span[@class="cur"]/text()')[0]

            # 机构家数、及占比
            ltrd_text = doctor_root.xpath('//div[@class="value_info"]//li[@class="long"]/p/text()')
            ltrd_text = ltrd_text[0] if ltrd_text else ''
            ltrd_re = re.findall(r"共(.+?)家主力机构.+?占流通A股(.+?)%", ltrd_text)
            minc, minr = ltrd_re[0] if ltrd_re else (0, 0)
            minc = int(minc)
            minr = float(minr)

            # 控盘情况
            cyw_text = doctor_root.xpath('//div[contains(text(),"控盘")]/text()')
            if cyw_text:
                cyw_text = cyw_text[0]
                cyw = re.findall(r"(.{2}控盘)", cyw_text)[0]
            else:
                cyw_text = '没有控盘'

            # 各分项比率
            tcr_text = doctor_root.xpath('//div[@class="box2wrap technical_score"]//span[@class="gray"]/text()')[0]
            tcr = int(re.findall(r"打败了(.+?)%的股票", tcr_text)[0])
            fdr_text = doctor_root.xpath('//div[@class="box2wrap funds_score"]//span[@class="gray"]/text()')[0]
            fdr = int(re.findall(r"打败了(.+?)%的股票", fdr_text)[0])
            mgr_text = doctor_root.xpath('//div[@class="box2wrap message_score"]//span[@class="gray"]/text()')[0]
            mgr = int(re.findall(r"打败了(.+?)%的股票", mgr_text)[0])
            inr_text = doctor_root.xpath('//div[@class="box2wrap trade_score"]//span[@class="gray"]/text()')[0]
            inr = int(re.findall(r"打败了(.+?)%的股票", inr_text)[0])
            bsr_text = doctor_root.xpath('//div[@class="box2wrap basic_score"]//span[@class="gray"]/text()')[0]
            bsr = int(re.findall(r"打败了(.+?)%的股票", bsr_text)[0])

            cursor.execute(insert_sql, (
                stock[0], stock[1], cpr, tcr, fdr, mgr, inr, bsr, cpl, cyw,
                minc, minr, today))
            conn.commit()
            time.sleep(0.25)
            # break
        pass

    @staticmethod
    def update_fundamental():

        conn = sqlite3.connect(Fundamental.FUNDA_DB)
        cursor = conn.cursor()
        tables = [
            Fundamental.FINA_TB,
            Fundamental.BASIC_TB,
            Fundamental.ANAL_FINA_TB,
            Fundamental.INDUSTRY_TB,
            Fundamental.OPERATE_TB,
            Fundamental.CONCEPT_TB,
        ]
        for table in tables:
            cursor.execute("drop table if exists '%s'" % table)
            pass

        Fundamental.download_basic()
        Fundamental.download_industry()
        Fundamental.download_operate()
        Fundamental.download_concept()
        Fundamental.download_finance_data()
        Fundamental.analyze_finance_data()
        pass

    @staticmethod
    def pick_by_fundamental():
        conn = sqlite3.connect(Fundamental.FUNDA_DB)
        cursor = conn.cursor()
        cursor.execute(''' select code,lrvr,lnpr,lkfnpr,
                                    rv,np,kfnp,rvr1,rvr2,rvr3,rvr4,
                                    npr1,npr2,npr3,npr4,
                                    kfnpr1,kfnpr2,kfnpr3,kfnpr4
                                    from '%s' ''' % (Fundamental.ANAL_FINA_TB, ))
        result = []
        for (code,lrvr,lnpr,lkfnpr,
             rv,np,kfnp,
             rvr1,rvr2,rvr3,rvr4,
             npr1,npr2,npr3,npr4,
             kfnpr1,kfnpr2,kfnpr3,kfnpr4) in cursor:
            if lkfnpr > 0 and kfnp > 0 and kfnpr1 > 0 and kfnpr2 >=0 and kfnpr3>=0 :
                result.append(code)

        # from synthesis import synthesis
        u_write_to_file(u_create_path_by_system('t_fina_good.txt'),result)
        # synthesis('/Users/hero101/Documents/t_fina_good.txt')
        return result
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
        fld1, fld2, fld3 = result[0] if result else ('', '', '')

        cursor.execute("select operate from '%s' where code='%s'" % (Fundamental.OPERATE_TB, code))
        result = [row for row in cursor]
        oper, = result[0] if result else ('',)

        cursor.execute("select cpr,tcr,fdr,mgr,inr,bsr,cpl,cyw,minc,minr from '%s' where code='%s'" % (
            Fundamental.DOCTOR_TB, code))
        result = [row for row in cursor]
        cpr, tcr, fdr, mgr, inr, bsr, cpl, cyw, minc, minr = result[0] if result else (0, 0, 0, 0, 0, 0, '', '', 0, 0)

        cursor.execute(
            "select concept,concept_e,concept_o,concept_jx,concept_ejx,concept_ojx from '%s' where code='%s'" % (
                Fundamental.CONCEPT_TB, code))

        concept_row = [row for row in cursor]
        if concept_row:
            concept_row = concept_row[0]
        concepts = []
        conceptsjx = []
        if concept_row:
            for i in range(3):
                if concept_row[i]:
                    concepts += concept_row[i].split(',')
                    conceptsjx += concept_row[i + 3].split('|||')
        # if concept:
        #     concepts.append(concept.split(','))
        # if concept_e:
        #     concepts.append(concept.split(','))
        # concepts = [row for row in cursor][0]
        # concept_ns = concepts[0].split(',')
        # concept_jxs = concepts[-1].split('|||')
        concept_jxinfo = []
        for i in range(len(concepts)):
            concept_jxinfo.append('   %-10s   %s' % (concepts[i], conceptsjx[i]))

        concept = [c for c in concepts[0:3] if c]

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

        jiejin_info = "最近1个月解禁情况：    无"
        cursor.execute(
            "select date_, type_,pct,cost from '%s' where code='%s' " % (
                Fundamental.JIEJIN_TB, code,))
        jiejin = [row for row in cursor]
        if jiejin:
            jiejin_info = "最近1个月解禁情况："
            for item in jiejin:
                item_info = '\n  %s     %s  解禁      占比%4.2f%%       成本   %s元' % item
                jiejin_info += item_info

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
主营业务:   %s
概念题材解析:   
%s
诊断信息：
    评级:%s   综合 %s%%   技术面 %s%%   资金面 %s%%   消息面 %s%%   行业面 %s%%   基本面 %s%%    
    主力机构 %s 家,  持有流通股 %s %%   主力控盘程度: %s 
%s
最近报告期业绩:  营收同比%s%%  净利润同比%s%%  扣非净利润同比%s%%
年度业绩:  营收          %8.5f亿     历年同比 %s%%   %s%%   %s%%   %s%%
          净利润        %8.5f亿     历年同比 %s%%   %s%%   %s%%   %s%%
          扣非净利润     %8.5f亿     历年同比 %s%%   %s%%   %s%%   %s%%
%s
%s
''' % (
            name, code, area, list_date, fld1, fld2, fld3, oper,
            '\n'.join(concept_jxinfo),
            cpl, cpr, tcr, fdr, mgr, inr, bsr, minc, minr, cyw,
            yzj_info,
            lrvr, lnpr, lkfnpr,
            rv, rvr4, rvr3, rvr2, rvr1,
            np, npr4, npr3, npr2, npr1,
            kfnp, kfnpr4, kfnpr3, kfnpr2, kfnpr1,
            zjc_info,
            jiejin_info
        )
        return info
        # print(info)

    @staticmethod
    def select_doctor_codes():
        concepts = u_read_input('data/hot_concepts')
        concepts_codes = Fundamental.get_codes_by_concept(concepts)
        conn = sqlite3.connect(Fundamental.FUNDA_DB)
        table_name = Fundamental.DOCTOR_TB
        cursor = conn.cursor()
        cursor.execute(
            '''select distinct code from '%s' where 
            cpr > 50 and 
            fdr > 30 and 
            bsr > 50 and 
            mgr > 30 and 
            inr >30 and 
            cpl != '减持' and 
            minc < 100 and 
            cyw != '没有控盘' ''' % (
                table_name,))
        codes = [row[0] for row in cursor if row[0] in concepts_codes]
        codes = Fundamental.remove_st(codes)

        return codes

    @staticmethod
    def select_yuzen_codes():
        conn = sqlite3.connect(Fundamental.FUNDA_DB)
        cursor = conn.cursor()
        cursor.execute(
            "select distinct code from '%s' where type='扭亏' or type='大幅上升' or type='预增' order by pct desc " % Fundamental.YUZJ_TB)
        codes = [row[0] for row in cursor]
        codes = Fundamental.remove_st(codes)
        codes = codes[0:800]
        return codes

    @staticmethod
    def daily_run2():

        conn = sqlite3.connect(Fundamental.FUNDA_DB)
        cursor = conn.cursor()
        tables = [
            Fundamental.DOCTOR_TB,

        ]
        for table in tables:
            cursor.execute("drop table if exists '%s'" % table)
            pass
        Fundamental.download_doctor()
        u_write_to_file(u_create_path_by_system('t_doctor_all.txt'), Fundamental.select_doctor_codes())
        cursor.execute("create table if not exists doc_hist as select * from %s where 1=0" % Fundamental.DOCTOR_TB)
        cursor.execute("delete from doc_hist where date='%s'" % u_day_befor(0))
        conn.commit()
        cursor.execute("insert into doc_hist select * from %s" % Fundamental.DOCTOR_TB)
        conn.commit()

        Fundamental.update_bellwether()

    @staticmethod
    def check_doctor():
        from mytushare import get_realtime_quotes
        overflow = 2.8
        codes = Fundamental.select_doctor_codes()
        if codes:
            results = {}
            df = get_realtime_quotes([key for key in codes])
            for index, row in df.iterrows():
                code = row['code']
                pre_close = float(row['pre_close'])
                price = float(row['price'])
                rcp = round((price / pre_close - 1) * 100, 2)
                if rcp > overflow:
                    results[code] = price
            u_write_to_file(u_create_path_by_system('t_doctor_check.txt'), results)
            return results
        return {}

    @staticmethod
    def check_yuzen():
        from mytushare import get_realtime_quotes
        overflow = 2.8
        codes = Fundamental.select_yuzen_codes()
        # print(len(codes))
        # codes = codes[0:30]
        if codes:
            results = {}
            df = get_realtime_quotes([key for key in codes])
            for index, row in df.iterrows():
                code = row['code']
                pre_close = float(row['pre_close'])
                price = float(row['price'])
                if pre_close != 0:
                    rcp = round((price / pre_close - 1) * 100, 2)
                    if rcp > overflow:
                        results[code] = price
            u_write_to_file(u_create_path_by_system('t_yuzen_check.txt'), results)
            return results
        return {}


if __name__ == '__main__':
    import cProfile

    # Fundamental.download_from_wencai()
    # Fundamental.jiejin()
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
    # cProfile.run('Fundamental.update_fundamental()')
    # cProfile.run('Fundamental.download_doctor()')
    # cProfile.run('Fundamental.daily_run2()')
    # Fundamental.jiejin()
    # Fundamental.update_bellwether()
    # Fundamental.download_operate()
    # Fundamental.download_concept()

    # Fundamental.pick_codes_by_concept(
    #     [
    #         # '国产替代',
    #         '新基建',
    #         # '新能源汽车'
    #         # # '医疗信息化',
    #
    #     ])
    # Fundamental.pick_by_fundamental()
    pass
