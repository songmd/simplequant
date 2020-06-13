import csv
import os
import talib as ta
import pandas as pd
import sqlite3
from sqlalchemy import create_engine
import tushare as ts
from utility import *

import datetime
import time

from fundamental import Fundamental

from mytushare import get_realtime_quotes


# 数据处理
class DataHandler(object):
    RAW_DB = 'data/raw_data.db'
    COOKECD_DB = 'data/cooked_data.db'
    CAL_TXT = 'data/cal.txt'

    #
    # 280万
    # @staticmethod
    # #   根据上市日期获取股票代码
    # def get_codes_by_market_time(begin_date, end_date, output):
    #     conn = sqlite3.connect(DataHandler.RAW_DB)
    #     cursor = conn.cursor()
    #
    #     time_cond = ""
    #     if begin_date and end_date:
    #         time_cond = " where list_date>='%s' and list_date<='%s'" % (begin_date, end_date)
    #     elif begin_date:
    #         time_cond = " where list_date>='%s'" % begin_date
    #     elif end_date:
    #         time_cond = " where list_date<='%s'" % end_date
    #
    #     cursor.execute("select symbol from basic" + time_cond)
    #     codes = set()
    #     for row in cursor:
    #         codes.add(row[0])
    #
    #     if output:
    #         u_write_to_file(output, codes)
    #     else:
    #         return codes

    # @staticmethod
    # #   获得所有股票的市值
    # def get_stocks_total_mv():
    #     conn = sqlite3.connect(DataHandler.RAW_DB)
    #     cursor = conn.cursor()
    #
    #     cursor.execute("select code,totals from basic")
    #     codes = set()
    #     for row in cursor:
    #         codes.add(row[0])
    #
    #     return codes

    #   日常批处理
    @staticmethod
    def run_daily_batch():
        # 先下载日历
        DataHandler.download_trade_cal()
        # 再更新复权因子
        DataHandler.download_all_adj_factor()
        # 日常处理下载
        DataHandler.daily_routine()

        # 下载日线行情
        DataHandler.download_daily()

    # 日常处理下载
    @staticmethod
    def daily_routine():
        engine = create_engine('sqlite:///' + DataHandler.RAW_DB)
        pro = ts.pro_api()
        df = pro.stock_basic()
        df.to_sql('basic', engine, if_exists='replace', index=False)
        days = u_read_input(DataHandler.CAL_TXT)
        df = pro.daily_basic(ts_code='', trade_date=days[-1])
        if df.empty:
            df = pro.daily_basic(ts_code='', trade_date=days[-2])
        df.to_sql('daily_basic', engine, if_exists='replace', index=False)

        df = pro.concept(src='ts')
        df.to_sql('concept', engine, if_exists='replace', index=False)

        pass

    # 概念股明显
    @staticmethod
    def download_concept_detail():
        engine = create_engine('sqlite:///' + DataHandler.RAW_DB)
        pro = ts.pro_api()
        df = pro.concept(src='ts')
        df.to_sql('concept', engine, if_exists='replace', index=False)

        concept_codes = {row['code'] for index, row in df.iterrows()}

        conn = sqlite3.connect(DataHandler.RAW_DB)
        cursor = conn.cursor()
        cursor.execute('''
                create table if not exists concept_detail
                (
                  id            TEXT,
                  concept_name  TEXT,
                  ts_code       TEXT,
                  name          TEXT,
                  in_date       TEXT,
                  out_date      TEXT
                );
        ''')
        cursor.execute("select distinct id from concept_detail")
        for row in cursor:
            concept_codes.discard(row[0])

        for code in concept_codes:
            df = pro.concept_detail(id=code, fields='concept_name,ts_code,name,in_date,out_date')
            for index, row in df.iterrows():
                cursor.execute("insert into concept_detail values (?,?,?,?,?,?)",
                               (
                                   code, row['concept_name'], row['ts_code'], row['name'], row['in_date'],
                                   row['out_date']))
            conn.commit()
            time.sleep(0.55)

            print(code)

        cursor.execute("create index if not exists cd_ts_code on concept_detail(ts_code)")

        pass

    # 下载日线行情
    @staticmethod
    def download_daily():
        conn = sqlite3.connect(DataHandler.RAW_DB)
        cursor = conn.cursor()

        # 已下载的日期
        cursor.execute("select distinct trade_date from daily")
        done_days = set()
        for row in cursor:
            done_days.add(row[0])

        # 复权因子表中有的日期，才能下载日线行情
        all_days = []
        cursor.execute("SELECT DISTINCT trade_date FROM adj_factor")
        for row in cursor:
            all_days.append(row[0])
        conn.close()

        # 复权因子表中有日期，而相关日线没下载，正是我们要的目标
        undone_days = [x for x in all_days if x not in done_days]
        factors = DataHandler.get_qfq_factor_for_days(undone_days)

        conn = sqlite3.connect(DataHandler.RAW_DB)
        cursor = conn.cursor()
        pro = ts.pro_api()
        for day in undone_days:
            df = pro.daily(trade_date=day)
            for index, row in df.iterrows():
                code = row['ts_code'][:-3]
                date = row['trade_date']
                cursor.execute("insert into daily values (?,?,?,?,?,?,?,?,?)", (
                    code, date, factors[code][date], row['open'], row['high'], row['low'], row['close'],
                    row['vol'] * 100, row['amount'] * 1000
                ))
            conn.commit()

    @staticmethod
    def download_trade_cal():
        pro = ts.pro_api()
        # print(datetime.date.today().strftime('%Y%m%d'))

        end_date = u_day_befor(0)
        now = datetime.datetime.now()
        if now.hour <= 15:
            end_date = u_day_befor(1)

        df = pro.trade_cal(exchange='', start_date=u_month_befor(42), end_date=end_date)
        cal = []
        for i in range(len(df)):
            if df['is_open'][i] == 1:
                # print(df['cal_date'][i])
                cal.append(df['cal_date'][i])

        u_write_to_file(DataHandler.CAL_TXT, cal)

    @staticmethod
    # 调用此方法之前，需要调用download_all_adj_factor 下载全部的复权因子
    def cvs_to_raw_db():

        factors = DataHandler.get_qfq_factor()
        conn = sqlite3.connect(DataHandler.RAW_DB)
        cursor = conn.cursor()
        cursor.execute('drop table if exists daily')
        cursor.execute(
            'create table if not exists daily (code TEXT,trade_date TEXT,adj_factor REAL,open_ REAL,high REAL,low REAL,close_ REAL,vol REAL, amount REAL)')
        filepath = "stock_data_r"
        files = os.listdir(filepath)
        files.sort()
        for filename in files:
            if filename.endswith(".txt"):
                code = filename[-10:-4]
                with open(filepath + '/' + filename, encoding='gb18030') as csv_reader:
                    print(filename)
                    reader = csv.reader(csv_reader, delimiter=',')

                    for row in reader:
                        if len(row) == 7:
                            date = row[0].replace('/', '')
                            cursor.execute("insert into daily values(?,?,?,?,?,?,?,?,?)",
                                           (
                                               code, date, factors[code][date], float(row[1]), float(row[2]),
                                               float(row[3]),
                                               float(row[4]), float(row[5]), float(row[6])))

        cursor.execute('create index daily_datecode on daily(code,trade_date);')

        conn.commit()

    # 获取全部复权因子
    # 此方法可随时调用，保持adj_factor表是最新
    @staticmethod
    def download_all_adj_factor():
        pro = ts.pro_api()
        conn = sqlite3.connect(DataHandler.RAW_DB)
        cursor = conn.cursor()
        cursor.execute(
            '''
                create table if not exists adj_factor
                (
                  ts_code    TEXT,
                  trade_date TEXT,
                  adj_factor FLOAT
                );

              '''
        )
        cursor.execute("SELECT DISTINCT trade_date FROM adj_factor")
        done_days = set()
        for row in cursor:
            done_days.add(row[0])
        conn.close()

        for day in u_read_input(DataHandler.CAL_TXT):
            engine = create_engine('sqlite:///' + DataHandler.RAW_DB)
            if day not in done_days:
                df = pro.adj_factor(ts_code='', trade_date=day)
                df.to_sql('adj_factor', engine, if_exists='append', index=False)
            print(day)
            pass
        conn = sqlite3.connect(DataHandler.RAW_DB)
        cursor = conn.cursor()
        cursor.execute("create index if not exists adj_datecode on adj_factor(ts_code, trade_date);")

    @staticmethod
    def get_qfq_factor():

        conn = sqlite3.connect(DataHandler.RAW_DB)
        cursor = conn.cursor()
        ret = {}
        cursor.execute("select * from adj_factor")
        for row in cursor:
            code = row[0][:-3]
            if code not in ret:
                ret[code] = {}
            ret[code][row[1]] = row[2]
        return ret

    @staticmethod
    def get_qfq_factor_for_days(days):

        conn = sqlite3.connect(DataHandler.RAW_DB)
        cursor = conn.cursor()

        ret = {}
        cursor.execute("select * from adj_factor where trade_date in (%s)" % u_format_list(days))
        for row in cursor:
            code = row[0][:-3]
            if code not in ret:
                ret[code] = {}
            ret[code][row[1]] = row[2]
        return ret

    main_index = {
        '000001.SH': '上证综指',
        '399001.SZ': '深证成指',
        '399006.SZ': '创业板指',
        '399005.SZ': '中小板指',
        '000300.SH': '沪深300',
        '000016.SH': '上证50',
        '000010.SH': '上证180',
        '000009.SH': '上证380',
        '399007.SZ': '深证300',
        '399008.SZ': '中小300',
        '000903.SH': '中证100',
        '000904.SH': '中证200',
        '000905.SH': '中证500',
        '000044.SH': '上证中盘',
    }

    @staticmethod
    def get_index_base(begin_date, end_date):
        conn = sqlite3.connect(DataHandler.COOKECD_DB)
        cursor = conn.cursor()
        table_name = 'daily_index'

        days = u_read_input(DataHandler.CAL_TXT)
        end_cal = ''
        begin_cal = ''
        for day in reversed(days):
            if end_cal == '' and day <= end_date:
                end_cal = day
            if begin_cal == '' and day <= begin_date:
                begin_cal = day
            if begin_cal and end_cal:
                break

        cursor.execute("select name,close_ from '%s' where trade_date='%s' " % (
            table_name, begin_cal))
        begin_inds = {row[0]: row[1] for row in cursor}
        cursor.execute("select name,close_ from '%s' where trade_date='%s' " % (
            table_name, end_cal))
        end_inds = {row[0]: row[1] for row in cursor}

        base_inds = {}
        for key in begin_inds:
            base_inds[key] = round((end_inds[key] / begin_inds[key] - 1) * 100, 2)

        return base_inds

    @staticmethod
    def download_index():
        conn = sqlite3.connect(DataHandler.COOKECD_DB)
        cursor = conn.cursor()
        cursor.execute('''
                            create table if not exists daily_index
                            (
                              name    TEXT,
                              trade_date    TEXT,
                              amount  FLOAT,
                              close_  FLOAT,                              
                              close_r FLOAT,
                              open_r  FLOAT,
                              high_r  FLOAT,
                              low_r   FLOAT,
                              trend   BIGINT,
                              ljt     FLOAT,
                              d1t     FLOAT,
                              k1t     FLOAT,
                              q3      FLOAT,
                              q5      FLOAT,
                              q10     FLOAT,
                              q20     FLOAT,
                              macd    FLOAT,
                              open_   FLOAT,
                              high    FLOAT,
                              low     FLOAT,
                              vol     FLOAT,
                              code    TEXT,
                              dif     FLOAT,
                              dea     FLOAT,
                              ema12   FLOAT,
                              ema26   FLOAT,

                              ma5     FLOAT,
                              ma10     FLOAT,
                              ma20     FLOAT,
                              ma30    FLOAT,
                              ma60     FLOAT,
                              ma120     FLOAT,
                              ma250     FLOAT,
                              ub     FLOAT,
                              mb     FLOAT,
                              lb     FLOAT,
                              ma      TEXT,                          
                              boll      TEXT,
                              bw     FLOAT                              
                              );
        ''')

        today = datetime.date.today().strftime('%Y%m%d')
        pro = ts.pro_api()
        for code in DataHandler.main_index:
            print(code)
            df = pro.index_daily(ts_code=code, start_date=u_month_befor(42), end_date=today)
            date = df['trade_date'].tolist()[::-1]
            open_ = df['open'].tolist()[::-1]
            high = df['high'].tolist()[::-1]
            low = df['low'].tolist()[::-1]
            close_ = df['close'].tolist()[::-1]
            vol = df['vol'].tolist()[::-1]
            amount = df['amount'].tolist()[::-1]

            length = len(close_)
            if length > 0:
                offset = 250
                # 计算macd
                temp_close = pd.Series([close_[0] if i < 0 else close_[i] for i in range(-offset, length)])

                # 布林线
                ub, mb, lb = ta.BBANDS(temp_close, timeperiod=20)
                ub = [round(x, 3) for x in ub[offset:]]
                mb = [round(x, 3) for x in mb[offset:]]
                lb = [round(x, 3) for x in lb[offset:]]

                # 均线
                ma5 = ta.MA(temp_close, timeperiod=5)
                ma10 = ta.MA(temp_close, timeperiod=10)
                ma20 = ta.MA(temp_close, timeperiod=20)
                ma30 = ta.MA(temp_close, timeperiod=30)
                ma60 = ta.MA(temp_close, timeperiod=60)
                ma120 = ta.MA(temp_close, timeperiod=120)
                ma250 = ta.MA(temp_close, timeperiod=250)

                ma5 = [round(x, 3) for x in ma5[offset:]]
                ma10 = [round(x, 3) for x in ma10[offset:]]
                ma20 = [round(x, 3) for x in ma20[offset:]]
                ma30 = [round(x, 3) for x in ma30[offset:]]
                ma60 = [round(x, 3) for x in ma60[offset:]]
                ma120 = [round(x, 3) for x in ma120[offset:]]
                ma250 = [round(x, 3) for x in ma250[offset:]]

                dif, dea, macd = ta.MACD(temp_close)
                ema12 = [round(x, 3) for x in ta.EMA(temp_close, timeperiod=12)[offset:]]
                ema26 = [round(x, 3) for x in ta.EMA(temp_close, timeperiod=26)[offset:]]
                dif = [round(x, 3) for x in dif[offset:]]
                dea = [round(x, 3) for x in dea[offset:]]
                macd = [round(x * 2, 3) for x in macd[offset:]]
                dd1 = [(abs(macd[i] - macd[i - 1]) + abs(macd[i - 1] - macd[i - 2])) / 2 if i >= 2 else 0
                       for i in range(length)]
                # 涨跌幅度
                close_r = [round((close_[i] / close_[i - 1] - 1) * 100, 2) if i >= 1 else 0 for i in
                           range(length)]
                open_r = [round((open_[i] / close_[i - 1] - 1) * 100, 2) if i >= 1 else 0 for i in
                          range(length)]
                low_r = [round((low[i] / close_[i - 1] - 1) * 100, 2) if i >= 1 else 0 for i in
                         range(length)]

                q3 = [round((close_[i] / (close_[i - 3] if i >= 3 else close_[0]) - 1) * 100, 2) for i in
                      range(length)]

                q5 = [round((close_[i] / (close_[i - 5] if i >= 5 else close_[0]) - 1) * 100, 2) for i in
                      range(length)]

                q10 = [round((close_[i] / (close_[i - 10] if i >= 10 else close_[0]) - 1) * 100, 2) for i in
                       range(length)]

                q20 = [round((close_[i] / (close_[i - 20] if i >= 20 else close_[0]) - 1) * 100, 2) for i in
                       range(length)]

                high_r = [round((high[i] / close_[i - 1] - 1) * 100, 2) if i >= 1 else 0 for i in
                          range(length)]

                ljt = [round(((macd[i] * 1755 / 8 + dea[i] * 351 - ema12[i] * 297 + ema26[i] * 325) / 28 /
                              close_[i] - 1) * 100, 2)
                       for i in range(length)]
                d1t = [round((
                                     ((macd[i] + dd1[i]) * 1755 / 8 + dea[i] * 351 - ema12[i] * 297 + ema26[
                                         i] * 325) / 28 / close_[
                                         i] - 1) * 100, 2)
                       for i in range(length)]

                k1t = [round((
                                     ((macd[i] - dd1[i]) * 1755 / 8 + dea[i] * 351 - ema12[i] * 297 + ema26[
                                         i] * 325) / 28 / close_[
                                         i] - 1) * 100, 2)
                       for i in range(length)]

                trend = [DataHandler._judge_macd(macd[i - 4], macd[i - 3], macd[i - 2], macd[i - 1], macd[i], dea[i],
                                                 ema12[i], ema26[i]) if i >= 4 else 0
                         for i in range(length)]

                ma = [DataHandler._judge_ma(
                    [ma5[i - 1], ma10[i - 1], ma20[i - 1], ma30[i - 1], ma60[i - 1], ma120[i - 1], ma250[i - 1]],
                    [ma5[i], ma10[i], ma20[i], ma30[i], ma60[i], ma120[i], ma250[i]]) if i >= 1 else ''
                      for i in range(length)]
                boll = [DataHandler._judge_boll(ub[i - 1], mb[i - 1], lb[i - 1], close_[i - 1], ub[i], mb[i],
                                                lb[i], close_[i]) if i >= 1 else ''
                        for i in range(length)]

                bw = [round((ub[i] - lb[i]) / mb[i], 3) for i in range(length)]

                for i in range(length):
                    cursor.execute(
                        "insert into daily_index values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            DataHandler.main_index[code], date[i], amount[i], close_[i], close_r[i], open_r[i],
                            high_r[i], low_r[i], trend[i], ljt[i], d1t[i], k1t[i], q3[i], q5[i], q10[i], q20[i],
                            macd[i], open_[i], high[i], low[i], vol[i], code, dif[i], dea[i], ema12[i],
                            ema26[i], ma5[i], ma10[i], ma20[i], ma30[i], ma60[i], ma120[i], ma250[i], ub[i], mb[i],
                            lb[i], ma[i],
                            boll[i], bw[i]
                        ))

        cursor.execute('create index ind_datecode on daily_index(code,trade_date);')
        conn.commit()
        pass

    @staticmethod
    def cook_raw_db(begin_date):

        if os.path.exists(DataHandler.COOKECD_DB):
            os.remove(DataHandler.COOKECD_DB)

        src_conn = sqlite3.connect(DataHandler.RAW_DB)
        src_cursor = src_conn.cursor()

        dest_conn = sqlite3.connect(DataHandler.COOKECD_DB)
        dest_cursor = dest_conn.cursor()
        dest_cursor.execute('''
                            create table daily
                            (
                              name    TEXT,
                              code    TEXT,
                              trade_date    TEXT,
                              close_r FLOAT,
                              open_r  FLOAT,
                              trend   BIGINT,
                              ljt     FLOAT,
                              d1t     FLOAT,
                              k1t     FLOAT,
                              q3      FLOAT,
                              q5      FLOAT,
                              q10     FLOAT,
                              q20     FLOAT,
                              macd    FLOAT,
                              high_r  FLOAT,
                              low_r   FLOAT,
                              open_   FLOAT,
                              high    FLOAT,
                              low     FLOAT,
                              close_  FLOAT,
                              vol     FLOAT,
                              amount  FLOAT,
                              dif     FLOAT,
                              dea     FLOAT,
                              ema12   FLOAT,
                              ema26   FLOAT,
                              ma5     FLOAT,
                              ma10     FLOAT,
                              ma20     FLOAT,
                              ma30     FLOAT,
                              ma60     FLOAT,
                              ma120     FLOAT,
                              ma250     FLOAT,
                              ub     FLOAT,
                              mb     FLOAT,
                              lb     FLOAT,
                              ma      TEXT,                          
                              boll      TEXT,
                              bw     FLOAT         
                            );
        ''')
        codes = DataHandler.get_codes()
        for code in codes:
            print(code)
            top_factor = 1
            src_cursor.execute("select adj_factor from daily where code='%s' order by trade_date desc limit 1" % code)
            for row in src_cursor:
                top_factor = row[0]
            src_cursor.execute(
                "select trade_date,adj_factor,open_,high,low,close_,vol,amount from daily where code='%s' and trade_date>='%s'" % (
                    code, begin_date))
            date = []
            open_ = []
            high = []
            low = []
            close_ = []
            vol = []
            amount = []
            for row in src_cursor:
                date.append(row[0])
                open_.append(round(row[1] * row[2] / top_factor, 4))
                high.append(round(row[1] * row[3] / top_factor, 4))
                low.append(round(row[1] * row[4] / top_factor, 4))
                close_.append(round(row[1] * row[5] / top_factor, 4))
                vol.append(int(row[6]))
                amount.append(float(row[7]))
            length = len(close_)
            if length > 0:
                # 计算macd
                offset = 250
                temp_close = pd.Series([close_[0] if i < 0 else close_[i] for i in range(-offset, length)])

                # 布林线
                ub, mb, lb = ta.BBANDS(temp_close, timeperiod=20)
                ub = [round(x, 3) for x in ub[offset:]]
                mb = [round(x, 3) for x in mb[offset:]]
                lb = [round(x, 3) for x in lb[offset:]]

                # 均线
                ma5 = ta.MA(temp_close, timeperiod=5)
                ma10 = ta.MA(temp_close, timeperiod=10)
                ma20 = ta.MA(temp_close, timeperiod=20)
                ma30 = ta.MA(temp_close, timeperiod=30)
                ma60 = ta.MA(temp_close, timeperiod=60)
                ma120 = ta.MA(temp_close, timeperiod=120)
                ma250 = ta.MA(temp_close, timeperiod=250)

                ma5 = [round(x, 3) for x in ma5[offset:]]
                ma10 = [round(x, 3) for x in ma10[offset:]]
                ma20 = [round(x, 3) for x in ma20[offset:]]
                ma30 = [round(x, 3) for x in ma30[offset:]]
                ma60 = [round(x, 3) for x in ma60[offset:]]
                ma120 = [round(x, 3) for x in ma120[offset:]]
                ma250 = [round(x, 3) for x in ma250[offset:]]

                dif, dea, macd = ta.MACD(temp_close)
                ema12 = [round(x, 3) for x in ta.EMA(temp_close, timeperiod=12)[offset:]]
                ema26 = [round(x, 3) for x in ta.EMA(temp_close, timeperiod=26)[offset:]]
                dif = [round(x, 3) for x in dif[offset:]]
                dea = [round(x, 3) for x in dea[offset:]]
                macd = [round(x * 2, 3) for x in macd[offset:]]
                dd1 = [(abs(macd[i] - macd[i - 1]) + abs(macd[i - 1] - macd[i - 2])) / 2 if i >= 2 else 0
                       for i in range(length)]
                # 涨跌幅度
                close_r = [round((close_[i] / close_[i - 1] - 1) * 100, 2) if i >= 1 else 0 for i in
                           range(length)]
                open_r = [round((open_[i] / close_[i - 1] - 1) * 100, 2) if i >= 1 else 0 for i in
                          range(length)]
                low_r = [round((low[i] / close_[i - 1] - 1) * 100, 2) if i >= 1 else 0 for i in
                         range(length)]

                q3 = [round((close_[i] / (close_[i - 3] if i >= 3 else close_[0]) - 1) * 100, 2) for i in
                      range(length)]

                q5 = [round((close_[i] / (close_[i - 5] if i >= 5 else close_[0]) - 1) * 100, 2) for i in
                      range(length)]

                q10 = [round((close_[i] / (close_[i - 10] if i >= 10 else close_[0]) - 1) * 100, 2) for i in
                       range(length)]

                q20 = [round((close_[i] / (close_[i - 20] if i >= 20 else close_[0]) - 1) * 100, 2) for i in
                       range(length)]

                high_r = [round((high[i] / close_[i - 1] - 1) * 100, 2) if i >= 1 else 0 for i in
                          range(length)]

                ljt = [round(((macd[i] * 1755 / 8 + dea[i] * 351 - ema12[i] * 297 + ema26[i] * 325) / 28 /
                              close_[i] - 1) * 100, 2)
                       for i in range(length)]
                d1t = [round((
                                     ((macd[i] + dd1[i]) * 1755 / 8 + dea[i] * 351 - ema12[i] * 297 + ema26[
                                         i] * 325) / 28 / close_[
                                         i] - 1) * 100, 2)
                       for i in range(length)]

                k1t = [round((
                                     ((macd[i] - dd1[i]) * 1755 / 8 + dea[i] * 351 - ema12[i] * 297 + ema26[
                                         i] * 325) / 28 / close_[
                                         i] - 1) * 100, 2)
                       for i in range(length)]

                trend = [DataHandler._judge_macd(macd[i - 4], macd[i - 3], macd[i - 2], macd[i - 1], macd[i], dea[i],
                                                 ema12[i], ema26[i]) if i >= 4 else 0
                         for i in range(length)]

                ma = [DataHandler._judge_ma(
                    [ma5[i - 1], ma10[i - 1], ma20[i - 1], ma30[i - 1], ma60[i - 1], ma120[i - 1], ma250[i - 1]],
                    [ma5[i], ma10[i], ma20[i], ma30[i], ma60[i], ma120[i], ma250[i]]) if i >= 1 else ''
                      for i in range(length)]
                boll = [DataHandler._judge_boll(ub[i - 1], mb[i - 1], lb[i - 1], close_[i - 1], ub[i], mb[i],
                                                lb[i], close_[i]) if i >= 1 else ''
                        for i in range(length)]

                bw = [round((ub[i] - lb[i]) / mb[i], 3) for i in range(length)]

                for i in range(length):
                    dest_cursor.execute(
                        "insert into daily values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            codes[code], code, date[i], close_r[i], open_r[i], trend[i], ljt[i], d1t[i],
                            k1t[i], q3[i], q5[i], q10[i], q20[i], macd[i], high_r[i], low_r[i], open_[i],
                            high[i], low[i], close_[i], vol[i], amount[i], dif[i], dea[i], ema12[i],
                            ema26[i], ma5[i], ma10[i], ma20[i], ma30[i], ma60[i], ma120[i], ma250[i], ub[i], mb[i],
                            lb[i], ma[i],
                            boll[i], bw[i]
                        ))

        dest_cursor.execute('create index datecode on daily(code,trade_date);')
        dest_conn.commit()

    @staticmethod
    def get_list_date():
        conn = sqlite3.connect(DataHandler.RAW_DB)
        cursor = conn.cursor()
        cursor.execute(
            "select symbol,list_date from basic")
        return {row[0]: row[1] for row in cursor}

    @staticmethod
    def get_monster(limit, month):
        from synthesis import synthesis
        conn = sqlite3.connect(DataHandler.COOKECD_DB)
        cursor = conn.cursor()
        list_date = DataHandler.get_list_date()
        result = []
        for key in list_date:
            if limit < 100:
                cursor.execute(
                    "select code, trade_date from daily where code ='%s' and trade_date>'%s' and trade_date >'%s' and q10>%s order by trade_date desc" % (
                        key, u_month_after(list_date[key], 2), u_month_befor(month), limit))
            else:
                cursor.execute(
                    "select code, trade_date from daily where code ='%s' and trade_date>'%s' and trade_date >'%s' and q20>%s order by trade_date desc" % (
                        key, u_month_after(list_date[key], 2), u_month_befor(month), limit))

            for code, date in cursor:
                result.append((code, date))
                break
        result.sort(key=lambda s: s[1], reverse=True)

        u_write_to_file('data/monster_%s_%s.txt' % (limit, month), [e[0] for e in result])
        u_write_to_file(u_create_path_by_system('t_monster_%s_%s.txt') % (limit, month), [e[0] for e in result])
        synthesis('data/monster_%s_%s.txt' % (limit, month))
        return result

    @staticmethod
    def get_codes():
        conn = sqlite3.connect(DataHandler.RAW_DB)
        cursor = conn.cursor()
        cursor.execute("SELECT symbol,name FROM basic")
        result = {}
        for row in cursor:
            result[row[0]] = row[1]
        return result

    # 判断布林线形态
    @staticmethod
    def _judge_boll(ub_, mb_, lb_, p_, ub, mb, lb, p):
        ret = []
        ret.append('ud%s' % int(ub > ub_))
        ret.append('md%s' % int(mb > mb_))
        ret.append('ld%s' % int(lb > lb_))
        if p > ub and p_ < ub_:
            # 价格向上突破上轨
            ret.append('ux1')
        if p > mb and p_ < mb_:
            # 价格向上突破中轨
            ret.append('mx1')
        if p > lb and p_ < lb_:
            # 价格向上突破下轨
            ret.append('lx1')

        if p < ub and p_ > ub_:
            # 价格向下突破上轨
            ret.append('ux0')
        if p < mb and p_ > mb_:
            # 价格向下突破中轨
            ret.append('mx0')
        if p < lb and p_ > lb_:
            # 价格向下突破下轨
            ret.append('lx0')

        return ' '.join(ret)

    # 判断均线形态
    @staticmethod
    def _judge_ma(ma_, ma):
        ret = []
        length = min(len(ma_), len(ma))
        for i in range(length):
            # i 线的方向向上还是向下
            ret.append('%sd%s' % (i + 1, int(ma[i] > ma_[i])))
        for i in range(length - 1):
            for j in range(i + 1, length):
                # i线与j线死叉
                if ma[j] > ma[i] and ma_[j] < ma_[i]:
                    ret.append('%s%sx0' % (i + 1, j + 1))
                # i线与j线金叉
                if ma[i] > ma[j] and ma_[i] < ma_[j]:
                    ret.append('%s%sx1' % (i + 1, j + 1))
        return ' '.join(ret)

    @staticmethod
    def _judge_macd(macd4, macd3, macd2, macd1, macd, dea, ema12, ema26):
        # 废掉参数macd4
        macd4 = macd3
        if macd > macd1 and macd1 > 0 and macd1 <= macd2 and macd2 <= macd3 and macd3 <= macd4:
            # 轧空棒
            return 4
        elif macd < macd1 and macd1 < 0 and macd1 >= macd2 and macd2 >= macd3 and macd3 >= macd4:
            # 杀多棒
            return -4
        elif macd > macd1 and macd < 0 and macd1 <= macd2 and macd2 <= macd3 and macd3 <= macd4:
            # 抽脚棒
            dd1 = (abs(macd - macd1) + abs(macd1 - macd2)) / 2
            dd2 = (abs(macd - macd1) + dd1) / 2
            dd3 = (dd1 + dd2) / 2
            sc1 = ((macd + dd1) * 1755 / 8 + dea * 351 - ema12 * 297 + ema26 * 325) / 28
            sa1 = ema12 * 11 / 13 + sc1 * 2 / 13
            sb1 = ema26 * 25 / 27 + sc1 * 2 / 27
            sdif1 = sa1 - sb1
            sdea1 = dea * 8 / 10 + sdif1 * 2 / 10
            sc2 = ((macd + dd1 + dd2) * 1755 / 8 + sdea1 * 351 - sa1 * 297 + sb1 * 325) / 28
            sa2 = sa1 * 11 / 13 + sc2 * 2 / 13
            sb2 = sb1 * 25 / 27 + sc2 * 2 / 27
            sdif2 = sa2 - sb2
            sdea2 = sdea1 * 8 / 10 + sdif2 * 2 / 10
            sc3 = ((macd + dd1 + dd2 + dd3) * 1755 / 8 + sdea2 * 351 - sa2 * 297
                   + sb2 * 325) / 28

            if sc1 < sc2 and sc2 < sc3:
                # 做多
                return 3
            else:
                # 盘整 不参与
                return 2
            pass
        elif macd < macd1 and macd > 0 and macd1 >= macd2 and macd2 >= macd3 and macd3 >= macd4:
            # 缩头棒
            dd1 = (abs(macd - macd1) + abs(macd1 - macd2)) / 2
            dd2 = (abs(macd - macd1) + dd1) / 2
            dd3 = (dd1 + dd2) / 2
            xc1 = ((macd - dd1) * 1755 / 8 + dea * 351 - ema12 * 297 + ema26 * 325) / 28
            xa1 = ema12 * 11 / 13 + xc1 * 2 / 13
            xb1 = ema26 * 25 / 27 + xc1 * 2 / 27
            xdif1 = xa1 - xb1
            xdea1 = dea * 8 / 10 + xdif1 * 2 / 10
            xc2 = ((macd - dd1 - dd2) * 1755 / 8 + xdea1 * 351 - xa1 * 297 + xb1 * 325) / 28
            xa2 = xa1 * 11 / 13 + xc2 * 2 / 13
            xb2 = xb1 * 25 / 27 + xc2 * 2 / 27
            xdif2 = xa2 - xb2
            xdea2 = xdea1 * 8 / 10 + xdif2 * 2 / 10
            xc3 = ((macd - dd1 - dd2 - dd3) * 1755 / 8 + xdea2 * 351 - xa2 * 297
                   + xb2 * 325) / 28
            if xc1 > xc2 and xc2 > xc3:
                # 下跌 做空
                return -3
            else:
                # 盘整
                return -2
        elif macd > macd1:
            return 1
        elif macd < macd1:
            return -1
        return 0

    row_count = 1

    attention_codes = {
        '603106': '恒银金融',
        '002716': '金贵银业',
        '600487': '恒通光电',
        '000531': '穗恒运A',
        '000927': '一汽夏利',
        '002917': '金奥博',
        '603601': '再升科技',
        '600519': '贵州茅台',
        '002415': '海康威视',
        '600019': '宝钢股份',
        '603959': '百利科技',
        '300119': '瑞普生物',
        '002378': '章源钨业',
        '603267': '鸿远电子',
        '002261': '拓维信息',
        '300292': '帝尔激光',
        '300776': '吴通控股',
        '300401': '花园生物',
        '000961': '中南建设',
        '603028': '赛福天',
        '300563': '神宇股份',
        '002902': '铭普光磁',
        '300543': '朗科智能',
        '600831': '广电网络',
        '600736': '苏州高新',
        '603559': '',
        '600525': '',
        '603121': '',
        '600155': '',
        '002281': '',
        '002315': '',
        '002547': '',
        '300245': '',
        '300366': '',

    }

    @staticmethod
    def create_attention():
        conn = sqlite3.connect(DataHandler.COOKECD_DB)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT trade_date FROM main.daily  order by trade_date desc limit %s" % DataHandler.row_count)
        days = [r[0] for r in cursor]

        cursor.execute("drop table if exists attention")

        sql = "create table attention as select name,code,trade_date,open_r,close_r,trend,ljt,d1t,k1t,ma,boll,bw,high_r,low_r,q3,q5,q10,q20 from daily where code in (%s) and trade_date in (%s)" % (
            u_format_list(DataHandler.attention_codes), u_format_list(days))
        cursor.execute(sql)
        # cursor.execute("alter table attention add name TEXT ")
        #
        # for code in DataHandler.attention_codes:
        #     cursor.execute(
        #         "update attention set name = '%s' where code = '%s'" % (DataHandler.attention_codes[code], code))
        # conn.commit()

        pass

    @staticmethod
    def create_attention_ex():
        day_count = 1
        attention_tables = {
            'macd_al': u_create_path_by_system('t_macd_all.txt'),
            'macd_jx': u_create_path_by_system('t_macd_jx.txt'),
            'acti_al': u_create_path_by_system('t_acti_all.txt'),
            'acti_jx': u_create_path_by_system('t_acti_jx.txt')
        }

        conn = sqlite3.connect(DataHandler.COOKECD_DB)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT trade_date FROM main.daily  order by trade_date desc limit %s" % day_count)
        days = [r[0] for r in cursor]

        for key in attention_tables:
            cursor.execute("drop table if exists %s" % key)
            codes = u_read_input(attention_tables[key])
            sql = "create table %s as select * from daily where code in (%s) and trade_date in (%s)" % (key,
                                                                                                        u_format_list(
                                                                                                            codes),
                                                                                                        u_format_list(
                                                                                                            days))
            cursor.execute(sql)
        pass

    @staticmethod
    def get_history_price(stocks, trading_date):
        conn = sqlite3.connect(DataHandler.RAW_DB)
        cursor = conn.cursor()
        result = {}
        for stock in stocks:
            cursor.execute(
                '''select code,close_ from main.daily 
                where trade_date<='%s' and code ='%s' 
                order by trade_date desc limit 1 ''' % (trading_date, stock))
            r = [row for row in cursor][0]
            result[r[0]] = r[1]
        return result

    @staticmethod
    def get_stock_concepts(codes):
        conn = sqlite3.connect(DataHandler.RAW_DB)
        cursor = conn.cursor()
        ret = []
        for code in codes:
            ts_code = code + ('.SH' if code[0] == '6' else '.SZ')
            cursor.execute("select concept_name from concept_detail where ts_code = '%s'" % ts_code)
            ret.append(','.join([row[0] for row in cursor if row[0]]))
        return ret

    @staticmethod
    def update_today_table():
        df = ts.get_today_all()

        conn = sqlite3.connect(DataHandler.COOKECD_DB)
        cursor = conn.cursor()

        for index, row in df.iterrows():
            code = row['code']
            print(code)
            rcp = row['changepercent']
            pre_close = row['settlement']
            if pre_close <= 0:
                pre_close = row['open']
            if pre_close <= 0:
                continue

            rcpo = round((row['open'] / pre_close - 1) * 100, 2)
            rcph = round((row['high'] / pre_close - 1) * 100, 2)
            rcpl = round((row['low'] / pre_close - 1) * 100, 2)
            rtor = round(row['turnoverratio'], 3)
            cursor.execute("update today set rcp=?,rcpo=?,rcph=?,rcpl=?,rtor=? where code=?",
                           (rcp, rcpo, rcph, rcpl, rtor, code))
        conn.commit()

    @staticmethod
    def monitor_macd(debug):

        import itchat

        itchat.auto_login(hotReload=True)
        groups = itchat.get_chatrooms(update=True)
        target_group = ''
        nickname = '三语股票测试' if debug else '三语股票'
        for group in groups:
            if group['NickName'] == nickname:
                target_group = group['UserName']
                break

        # itchat.send("Hello, i'am robot", toUserName=target_group)

        overflow = 0.8
        macd_target = 'macd_target.txt'
        conn = sqlite3.connect(DataHandler.COOKECD_DB)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT trade_date FROM main.daily  order by trade_date desc limit 1")
        days = [r[0] for r in cursor]
        codes = u_read_input(macd_target)
        sql = "select code,ljt,d1t,macd,ma,boll from daily where code in (%s) and trade_date in (%s) and trend < 0 and ljt < 9.0 and d1t<9.0" % (
            u_format_list(codes), u_format_list(days))
        cursor.execute(sql)
        targets = {}
        for row in cursor:
            targets[row[0]] = (row[1], row[2], row[3], row[4], row[5])
        msg = '股票代码: %s\n当前价格: %s\n当前涨幅: %s%%\n多空临界: %s%%\n多头:        %s%%\nmacd:       %s\n均线:        %s\n布林线:     %s\n时间:        %s %s'
        while targets:

            df = ts.get_realtime_quotes([key for key in targets])
            for index, row in df.iterrows():
                code = row['code']
                if code in targets:
                    pre_close = float(row['pre_close'])
                    price = float(row['price'])
                    rcp = round((price / pre_close - 1) * 100, 2)
                    if rcp > targets[code][0] + overflow:
                        print(code, price, )
                        itchat.send(msg % (
                            code, price, rcp, targets[code][0], targets[code][1], targets[code][2],
                            targets[code][3],
                            targets[code][4], row['date'], row['time']), toUserName=target_group)

                        targets.pop(code)
            if debug:
                break
            else:
                time.sleep(5)

        pass

    @staticmethod
    def check_macd():
        from fundamental import Fundamental
        concepts = u_read_input('data/hot_concepts')
        concept_codes = Fundamental.get_codes_by_concept(concepts)

        overflow = 1.8
        conn = sqlite3.connect(DataHandler.COOKECD_DB)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT trade_date FROM main.daily  order by trade_date desc limit 1")
        days = [r[0] for r in cursor]
        codes1 = u_read_input(u_create_path_by_system('t_macd_all.txt'))
        codes2 = u_read_input(u_create_path_by_system('t_macd_jx.txt'))
        sql = "select code,ljt,d1t from daily where code in (%s) and trade_date in (%s) and trend < 0 and ljt < 9.0 and d1t<9.0" % (
            u_format_list([e for e in concept_codes if (e in codes1 or e in codes2)]), u_format_list(days))
        cursor.execute(sql)
        targets = {}
        for row in cursor:
            targets[row[0]] = (row[1], row[2])
        # print(len(targets))
        results = {}
        if targets:
            df = get_realtime_quotes([key for key in targets])
            for index, row in df.iterrows():
                code = row['code']
                if code in targets:
                    pre_close = float(row['pre_close'])
                    price = float(row['price'])
                    rcp = round((price / pre_close - 1) * 100, 2)
                    # print(rcp,targets[code][1])
                    if rcp > targets[code][0] + overflow:
                        results[code] = price

        u_write_to_file(u_create_path_by_system('t_macd_check.txt'), results)
        return results

    @staticmethod
    def select_today_macd():
        targets = {
            'macd_al': u_create_path_by_system('t_macd_all.txt'),
            'macd_jx': u_create_path_by_system('t_macd_jx.txt')
        }
        from fundamental import Fundamental
        concepts = u_read_input('data/hot_concepts')
        concept_codes = Fundamental.get_codes_by_concept(concepts)

        conn = sqlite3.connect(DataHandler.COOKECD_DB)
        cursor = conn.cursor()
        result = []
        for key in targets:
            codes = u_read_input(targets[key])
            cursor.execute(
                "SELECT code FROM main.today  where code in (%s) and trend>=2" % u_format_list(codes))
            result += [row[0] for row in cursor if row[0] not in result]

        cursor.execute(
            "SELECT code FROM main.today  where trend>=2 and close_r>0.3 and (boll like '%x1%' or ma like '%x1%')")

        result += [row[0] for row in cursor if row[0] not in result]

        result = Fundamental.remove_st(result)

        result = [e for e in result if e in concept_codes]

        u_write_to_file(u_create_path_by_system('t_macd_today.txt'), result)
        pass

    @staticmethod
    # 获取指数技术指标信息
    def get_index_tech_index_info(name):
        conn = sqlite3.connect(DataHandler.COOKECD_DB)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT DISTINCT trade_date FROM main.daily_index  order by trade_date desc limit 1")
        today = [row[0] for row in cursor][0]

        cursor.execute(
            '''select amount,close_r,trend,
            ma,boll,ljt,d1t,close_,ma5,ma10,ma20,ma30,ma60,ma120,ma250,ub,mb,lb
            from daily_index where name = '%s' and trade_date = '%s' ''' % (name, today))

        result = [row for row in cursor]
        if not result:
            return
        (amount, close_r, trend, ma, boll, ljt, d1t,
         close_, ma5, ma10, ma20, ma30, ma60, ma120, ma250, ub, mb, lb) = result[0]

        tech_info = '''
    行情:   涨幅 %s%%   成交 %s 亿 
    均线：   %s    收盘距5日线 %s%%  10日线 %s%%  20日线 %s%%  30日线 %s%%  60日线 %s%%  120日线 %s%%  250日线 %s%%
    macd:   方向(%s)  多空临界值 %s%%   多头位 %s%%
    boll线： %s    收盘距上轨 %s%%   中轨 %s%%    下轨 %s%%
    ''' % (close_r, round(amount / 100000, 1),
           ma, u_calc_ratio(ma5, close_), u_calc_ratio(ma10, close_), u_calc_ratio(ma20, close_),
           u_calc_ratio(ma30, close_),
           u_calc_ratio(ma60, close_), u_calc_ratio(ma120, close_),
           u_calc_ratio(ma250, close_),
           trend, ljt, d1t,
           boll, u_calc_ratio(ub, close_), u_calc_ratio(mb, close_), u_calc_ratio(lb, close_),
           )
        return tech_info

    @staticmethod
    # 获取技术指标信息
    def get_tech_index_info(code):
        conn = sqlite3.connect(DataHandler.COOKECD_DB)
        cursor = conn.cursor()

        cursor.execute(
            '''select vol_r,close_r,t_r_f,t_mv,c_mv,trend,
            ma,boll,ljt,d1t,close_,ma5,ma10,ma20,ma30,ma60,ma120,ma250,ub,mb,lb
            from today where code = '%s' ''' % (code,))

        result = [row for row in cursor]
        if not result:
            return
        (vol_r, close_r, t_r_f, t_mv, c_mv, trend, ma, boll, ljt, d1t,
         close_, ma5, ma10, ma20, ma30, ma60, ma120, ma250, ub, mb, lb) = result[0]

        tech_info = '''行情:   涨幅 %s%%   量比 %s   换手 %s  
流通市值 %s亿   总市值 %s亿
均线：   %s    收盘价距5日线 %s%%  10日线 %s%%  20日线 %s%%  30日线 %s%%  60日线 %s%%  120日线 %s%%  250日线 %s%%
macd:   方向(%s)  多空临界值 %s%%   多头位 %s%%
boll线： %s    收盘价距上轨 %s%%   中轨 %s%%    下轨 %s%%
''' % (close_r, vol_r, t_r_f, round(c_mv / 10000, 3), round(t_mv / 10000, 3),
       ma, u_calc_ratio(ma5, close_), u_calc_ratio(ma10, close_), u_calc_ratio(ma20, close_),
       u_calc_ratio(ma30, close_),
       u_calc_ratio(ma60, close_), u_calc_ratio(ma120, close_),
       u_calc_ratio(ma250, close_),
       trend, ljt, d1t,
       boll, u_calc_ratio(ub, close_), u_calc_ratio(mb, close_), u_calc_ratio(lb, close_),
       )
        return tech_info
        # print(tech_info)

    @staticmethod
    def create_today_super_stock():
        conn = sqlite3.connect(DataHandler.COOKECD_DB)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT code FROM today where list_date < '%s' and close_r >= 8.0 order by close_r desc " % (
                u_day_befor(6),))
        u_write_to_file(u_create_path_by_system('t_ss_today.txt'), [row[0] for row in cursor])
        pass

    @staticmethod
    def create_today_table():

        # dates = u_read_input(DataHandler.CAL_TXT)
        # today = dates[-1]
        # now = datetime.datetime.today()
        # if now. == today and
        conn = sqlite3.connect(DataHandler.COOKECD_DB)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT trade_date FROM main.daily  order by trade_date desc limit 1")
        today = [row[0] for row in cursor][0]
        cursor.execute("drop table if exists today")
        conn.close()

        engine_c = create_engine('sqlite:///' + DataHandler.COOKECD_DB)
        engine_r = create_engine('sqlite:///' + DataHandler.RAW_DB)

        # sql = "create table today as select * from daily where trade_date='%s'" % today

        sql = "select name,code,close_r,open_r,trend,ma,boll,bw,q3,q5,q10,q20,ljt,d1t,macd,dif,close_,ma5,ma10,ma20,ma30,ma60,ma120,ma250,high_r,low_r,ub,mb,lb from daily where trade_date='%s'" % today

        df_today = pd.read_sql_query(sql, engine_c, index_col='code')

        df_basic = pd.read_sql_query("select symbol,list_date from basic", engine_r, index_col='symbol')

        df_daily_basic = pd.read_sql_query(
            "select ts_code,turnover_rate_f,volume_ratio,pe,circ_mv,total_mv from daily_basic", engine_r,
            index_col='ts_code')

        list_date = []
        turnover_rate_f = []
        volume_ratio = []
        pe = []
        circ_mv = []
        total_mv = []

        # df_today.insert(0, 'new_colomn', [])
        for index, row in df_today.iterrows():
            ts_code = index + '.SH' if index[0] == '6' else index + '.SZ'
            list_date.append(df_basic['list_date'][index])
            turnover_rate_f.append(df_daily_basic.get('turnover_rate_f').get(ts_code, 0))
            volume_ratio.append(df_daily_basic.get('volume_ratio').get(ts_code, 0))
            pe.append(df_daily_basic.get('pe').get(ts_code, 0))
            circ_mv.append(df_daily_basic.get('circ_mv').get(ts_code, 0))
            total_mv.append(df_daily_basic.get('total_mv').get(ts_code, 0))

        df_today.insert(1, 'vol_r', volume_ratio)
        df_today.insert(3, 't_r_f', turnover_rate_f)
        df_today.insert(16, 'pe', pe)
        df_today.insert(16, 'c_mv', circ_mv)
        df_today.insert(16, 't_mv', total_mv)
        df_today.insert(16, 'list_date', list_date)
        df_today.insert(16, 'concept', DataHandler.get_stock_concepts(df_today.index))

        length = len(list_date)
        # 实时涨幅
        df_today.insert(6, 'rcp', [0.0 for x in range(length)])
        df_today.insert(7, 'rcpo', [0.0 for x in range(length)])
        df_today.insert(8, 'rcph', [0.0 for x in range(length)])
        df_today.insert(9, 'rcpl', [0.0 for x in range(length)])
        df_today.insert(10, 'rtor', [0.0 for x in range(length)])

        df_today.to_sql('today', engine_c, if_exists='replace', index=True)

        pass


def profile_run():
    DataHandler.run_daily_batch()
    DataHandler.download_concept_detail()


def profile_run1():
    DataHandler.cook_raw_db(u_month_befor(42))
    DataHandler.create_today_table()
    # DataHandler.select_today_macd()
    DataHandler.download_index()
    DataHandler.create_attention_ex()


def dh_daily_run():
    DataHandler.run_daily_batch()
    # DataHandler.download_concept_detail()
    DataHandler.cook_raw_db(u_month_befor(42))
    DataHandler.create_today_table()
    DataHandler.create_today_super_stock()
    DataHandler.select_today_macd()
    DataHandler.download_index()
    # DataHandler.get_monster(90, 24)
    # DataHandler.get_monster(80, 12)
    # DataHandler.get_monster(70, 12)
    # DataHandler.get_monster(60, 12)
    # DataHandler.create_attention_ex()


def dh_check_macd():
    return DataHandler.check_macd()


def dh_get_monster():
    DataHandler.get_monster(150, 24)
    DataHandler.get_monster(90, 24)
    DataHandler.get_monster(80, 12)
    DataHandler.get_monster(60, 12)


def profile_run2():
    DataHandler.update_today_table()


def profile_run3():
    # create_index_adj_factor()
    # raw_to_qfq()
    # cvs_to_raw_db()
    # DataHandler.process_raw_db()
    # DataHandler.daily_download('20190529')
    # DataHandler.cvs_to_raw_db()
    # DataHandler.cook_raw_db()
    # DataHandler.cook_raw_db()
    # DataHandler.download_concept_detail()

    # DataHandler.daily_routine()

    # DataHandler.download_index()

    engine = create_engine('sqlite:///' + DataHandler.RAW_DB)
    pro = ts.pro_api()
    df = pro.stk_holdertrade()
    df.to_sql('zjc', engine, if_exists='replace', index=False)

    print(df)
    # # df = pro.index_basic(market='SSE')
    # df = pro.index_basic(market='SZSE')
    # for index, row in df.iterrows():
    #     print(row['ts_code'], row['name'])
    #
    # # print(df)
    # df = pro.index_dailybasic(trade_date='20190530')

    # print(df)


def profile_run4():
    # df = ts.get_realtime_quotes('600000')
    # print(df)
    # df = ts.get_realtime_quotes('600372')
    # # df = ts.get_realtime_quotes(['000581','603106'])
    # print(df)
    DataHandler.check_macd()
    # DataHandler.check_macd()


if __name__ == '__main__':
    import cProfile

    # df = ts.get_hist_data('603106', start='2019-07-17',end='2019-07-19',ktype='5')
    # print(df)

    # cProfile.run('DataHandler.monitor_macd(True)')
    # DataHandler.monitor_macd(False)
    # df = ts.get_realtime_quotes(['000980', '000981'])
    # print(df)
    # cProfile.run('profile_run4()')
    # profile_run4()

    # DataHandler.select_today_macd()
    # df = ts.get_today_all()
    # print(df)
    # import  xlrd
    # book = xlrd.open_workbook('2019-07-21.xlsx')
    # from openpyxl import load_workbook

    # wb = load_workbook(filename='2019-07-21-1.xls', read_only=True)
    # df = pd.read_excel('2019-07-21-1.xls')

    # engine = create_engine('sqlite:///' + DataHandler.COOKECD_DB)
    #
    # df.to_sql('yeji', engine, if_exists='replace', index=True)
    # xml_to_db()
    # DataHandler.daily_routine()
    # DataHandler.create_today_table()
    # DataHandler.select_today_macd()
    # ts.set_token('96242c965655948aef082bf119dc45d6f9ee05625306e2e130d1f056')

    # pro = ts.pro_api()
    #
    # df = pro.express(ts_code='603106.SH', start_date='20170101', end_date='20190701')
    #
    # engine = create_engine('sqlite:///' + DataHandler.RAW_DB)
    # df.to_sql('fina', engine, if_exists='replace', index=True)
    # print(df)
    # DataHandler.get_tech_index_info('603106')
    # DataHandler.create_today_table()
    # DataHandler.select_today_macd()
    # DataHandler.create_today_super_stock()
    # DataHandler.run_daily_batch()
    # DataHandler.create_today_table()
    # dh_daily_run()
    # monster = DataHandler.get_monster(80, 12)
    # print(monster)
    # dh_get_monster()
    # DataHandler.select_today_macd()
    # pass
