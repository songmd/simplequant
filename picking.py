from dataprocess import DataHandler
from utility import *
import sqlite3
import tushare as ts
from fundamental import Fundamental
from mytushare import get_realtime_quotes

PICKING_DB = 'data/picking.db'
WAVE_SCOPE_THRESHOLD = 14
ACTIVITY_THRESHOLD = 31
WAVE_COUNT_THRESHOLD = 12
LIMIT_COUNT_THRESHOLD = 0.02


def get_wave(scope, vct, is_index=False):
    result = []
    upstaying = 0
    for index in range(0, len(vct)):
        if index >= 1 and (vct[index] / vct[index - 1] - 1) * 100 > 9:
            upstaying += 1

        last_close = vct[index]
        if not result:
            result.append(index)
            continue
        # 与最后一个相同，去掉
        if vct[result[-1]] == vct[index]:
            continue
        # 是第二个增加
        if len(result) < 2:
            result.append(index)
            continue
        # 同向减
        if vct[result[-1]] > vct[result[-2]] and vct[index] > vct[result[-1]]:
            result.pop()
            result.append(index)
            continue
        # 同向增
        if vct[result[-1]] < vct[result[-2]] and vct[index] < vct[result[-1]]:
            result.pop()
            result.append(index)
            continue

        # 异向
        # 涨跌幅小于scope忽略
        if abs(1 - vct[result[-1]] / vct[result[-2]]) * 100 < scope:
            result.pop()
            result.append(index)
            if len(result) >= 3:
                if vct[result[-3]] < vct[result[-2]] and vct[result[-2]] < vct[result[-1]]:
                    result.pop(-2)
                elif vct[result[-3]] > vct[result[-2]] and vct[result[-2]] > vct[result[-1]]:
                    result.pop(-2)
            if vct[result[-2]] == vct[result[-1]]:
                result.pop()
        else:
            result.append(index)
    if len(result) >= 3:
        if abs(1 - vct[result[-1]] / vct[result[-2]]) * 100 < scope:
            result.pop()

    up = []
    down = []
    last_wave = 0
    last_dif = 0
    for index in range(len(result) - 1):
        r = round((vct[result[index + 1]] / vct[result[index]] - 1) * 100, 2)
        last_wave = r
        if r > 0:
            up.append(r)
        else:
            down.append(r)
    if up and not is_index:
        up.pop(up.index(max(up)))
    # if down:
    #     down.pop(down.index(min(down)))
    max_up = 0
    min_down = 0
    avg_up = 0
    avg_down = 0
    # 平均涨停数
    avg_limit = 0
    l_close_p = 0
    last_duration = 0
    high_dif = 0
    low_dif = 0
    if up:
        max_up = max(up)
        avg_up = round(sum(up) / len(up), 2)
    if down:
        min_down = min(down)
        avg_down = round(sum(down) / len(down), 2)

    sideway_duration = 0
    activity = 0
    if result:
        avg_limit = round(upstaying / len(vct), 2)
        activity = round(len(vct) / (len(result)), 2)
        last_dif = round((vct[-1] / vct[result[-1]] - 1) * 100, 2)
        high_dif = round((vct[-1] / max(vct) - 1) * 100, 2)
        low_dif = round((vct[-1] / min(vct) - 1) * 100, 2)
        if len(result) >= 2:
            last_duration = len(vct) - result[-2]
        else:
            last_duration = len(vct) - result[-1]
        sideway_duration = len(vct) - result[-1]
    if len(vct) >= 2:
        l_close_p = round((vct[-1] / vct[-2] - 1) * 100, 2)

    return result, len(
        result), activity, avg_limit, max_up, avg_up, min_down, avg_down, last_wave, l_close_p, high_dif, low_dif, last_dif, last_duration, sideway_duration


def verify(code, scope, begin_date, end_date):
    conn = sqlite3.connect(DataHandler.COOKECD_DB)
    cursor = conn.cursor()
    cursor.execute(
        "select close_,trade_date from main.daily where code = '%s' and trade_date>= '%s' and trade_date <='%s'" % (
            code, begin_date, end_date))
    close_ = []
    dates = []
    for row in cursor:
        close_.append(row[0])
        dates.append(row[1])
    result, waves, activity, avg_limit, max_up, avg_up, min_down, avg_down, last_wave, l_close_p, high_dif, low_dif, last_dif, last_duration = get_wave(
        scope, close_)
    for index in result:
        print(dates[index])


def stat_index(scopes, begin_date, end_date):
    print('stat_index', begin_date, end_date, '.....')
    conn_res = sqlite3.connect(PICKING_DB)
    cursor_res = conn_res.cursor()

    insert_sql = "insert into 'index' values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

    cursor_res.execute("drop table if exists 'index'")
    cursor_res.execute(
        '''create table if not exists 'index' (
        name TEXT,scope REAL,date_range TEXT,waves INT,activity REAL,
        max_up REAL,avg_up REAL,min_down REAL,avg_down REAL,last_wave REAL ,l_close_p REAL ,
        high_dif REAL,low_dif REAL,last_dif REAL,last_duration INT,sideway_duration INT,detail_dur TEXT,detail_scope TEXT)''')

    conn = sqlite3.connect(DataHandler.COOKECD_DB)
    cursor = conn.cursor()
    for code in DataHandler.main_index:
        name = DataHandler.main_index[code]
        cursor.execute(
            "select close_,trade_date from daily_index where name = '%s' and trade_date>= '%s' and trade_date <='%s'" % (
                name, begin_date, end_date))
        close_s = []
        date_s = []
        for row in cursor:
            close_s.append(row[0])
            date_s.append(row[1])
        for scope in scopes:
            (result, waves, activity, avg_limit, max_up, avg_up, min_down, avg_down, last_wave, l_close_p,
             high_dif, low_dif, last_dif, last_duration, sideway_duration) = get_wave(
                scope, close_s, True)
            details_dur = []
            details_scope = []
            if result:
                for i in range(1, len(result)):
                    details_dur.append('%6s' % (result[i] - result[i - 1],))
                    details_scope.append('%6s' % (u_calc_ratio(close_s[result[i - 1]], close_s[result[i]]),))

            cursor_res.execute(insert_sql,
                               (
                                   name, scope, '%s-%s' % (begin_date, end_date), waves, activity, max_up, avg_up,
                                   min_down,
                                   avg_down,
                                   last_wave, l_close_p, high_dif, low_dif, last_dif, last_duration, sideway_duration,
                                   '  '.join(reversed(details_dur)),
                                   '  '.join(reversed(details_scope))))
    conn_res.commit()
    pass


def stat_activity(table_name, replace, scope, begin_date, end_date):
    print('stat_activity', begin_date, end_date, '.....')
    conn_res = sqlite3.connect(PICKING_DB)
    cursor_res = conn_res.cursor()
    # 股性活跃度表
    # table_name = 'activity'
    insert_sql = "insert into '%s' values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" % table_name
    if replace:
        cursor_res.execute("drop table if exists '%s'" % table_name)
    cursor_res.execute(
        '''create table if not exists '%s' (
        code TEXT,date_range TEXT,waves INT,activity REAL,avg_limit REAL,
        max_up REAL,avg_up REAL,min_down REAL,avg_down REAL,last_wave REAL ,l_close_p REAL ,
        high_dif REAL,low_dif REAL,last_dif REAL,last_duration INT,sideway_duration INT,detail_dur TEXT,detail_scope TEXT)''' % table_name)

    conn = sqlite3.connect(DataHandler.COOKECD_DB)
    cursor = conn.cursor()
    for code in DataHandler.get_codes():
        cursor.execute(
            "select close_,trade_date from main.daily where code = '%s' and trade_date>= '%s' and trade_date <='%s'" % (
                code, begin_date, end_date))
        close_s = []
        date_s = []
        for row in cursor:
            close_s.append(row[0])
            date_s.append(row[1])
        (result, waves, activity, avg_limit, max_up, avg_up, min_down, avg_down, last_wave, l_close_p,
         high_dif, low_dif, last_dif, last_duration, sideway_duration) = get_wave(
            scope, close_s)
        details_dur = []
        details_scope = []
        if result:

            for i in range(1, len(result)):
                details_dur.append('%6s' % (result[i] - result[i - 1],))
                details_scope.append('%6s' % (u_calc_ratio(close_s[result[i - 1]], close_s[result[i]]),))

        cursor_res.execute(insert_sql,
                           (
                               code, '%s-%s' % (begin_date, end_date), waves, activity, avg_limit, max_up, avg_up,
                               min_down,
                               avg_down,
                               last_wave, l_close_p, high_dif, low_dif, last_dif, last_duration, sideway_duration,
                               '  '.join(reversed(details_dur)),
                               '  '.join(reversed(details_scope))))
    conn_res.commit()
    pass


def analyze_activity(table_name, activity, avg_limit, count_limit):
    print('analyze_activity...')
    conn = sqlite3.connect(PICKING_DB)
    cursor = conn.cursor()
    cursor.execute("select distinct code from '%s'" % table_name)
    codes = [row[0] for row in cursor]
    result = []
    for code in codes:
        cursor.execute("select activity,avg_limit from '%s' where code = '%s'" % (table_name, code))
        select = True
        count = 0
        for row in cursor:
            count += 1
            if row[0] > activity or row[0] == 0 or row[1] < avg_limit:
                select = False
        if select and count > count_limit:
            result.append(code)
    result = Fundamental.remove_st(result)
    u_write_to_file(u_create_path_by_system('t_acti_jx.txt'), result)


def calc_rps():
    codes = DataHandler.get_codes()
    conn = sqlite3.connect(DataHandler.COOKECD_DB)
    cursor = conn.cursor()
    rps_raw_data = []

    def calc_ratio(pre, now):
        return round((now / pre - 1) * 100, 3)

    for code in codes:
        cursor.execute(
            "select close_ from main.daily where code = '%s' order by trade_date desc limit 250" % (
                code,))
        close_s = [row[0] for row in cursor]
        len_close = len(close_s)
        if len_close <= 0:
            continue
        len_close -= 1
        # if len(close_s) != 250:
        #     continue
        rps_raw_data.append([code,
                             calc_ratio(close_s[min(4, len_close - 1)], close_s[0]),
                             calc_ratio(close_s[min(9, len_close)], close_s[0]),
                             calc_ratio(close_s[min(19, len_close)], close_s[0]),
                             calc_ratio(close_s[min(29, len_close)], close_s[0]),
                             calc_ratio(close_s[min(59, len_close)], close_s[0]),
                             calc_ratio(close_s[min(119, len_close)], close_s[0]),
                             calc_ratio(close_s[min(249, len_close)], close_s[0]), len_close == 249])
    if not rps_raw_data:
        return
    length = len(rps_raw_data)

    rps_s = []
    for i in range(7):
        rps_s.append({})
        rps_raw_data.sort(key=lambda x: x[i + 1] if x[8] else -10000, reverse=True)
        for index, value in enumerate(rps_raw_data, start=1):
            rps_s[i][value[0]] = [round((1 - index / length) * 100, 3) if value[8] else -1, value[i + 1]]

    conn_res = sqlite3.connect(PICKING_DB)
    cursor_res = conn_res.cursor()
    table_name = 'rps'
    cursor_res.execute("drop table if exists '%s'" % table_name)
    cursor_res.execute(
        '''create table if not exists '%s'(
            code TEXT,name TEXT,
            rps5 REAL,r5 REAL,
            rps10 REAL,r10 REAL,
            rps20 REAL,r20 REAL,
            rps30 REAL,r30 REAL,
            rps60 REAL,r60 REAL,
            rps120 REAL,r120 REAL,
            rps250 REAL,r250 REAL)''' % table_name)
    insert_sql = "insert into '%s' values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" % table_name
    for code in codes:
        if code in rps_s[0]:
            cursor_res.execute(insert_sql,
                               (code, codes[code], rps_s[0][code][0], rps_s[0][code][1], rps_s[1][code][0],
                                rps_s[1][code][1], rps_s[2][code][0], rps_s[2][code][1],
                                rps_s[3][code][0], rps_s[3][code][1], rps_s[4][code][0], rps_s[4][code][1],
                                rps_s[5][code][0], rps_s[5][code][1],
                                rps_s[6][code][0], rps_s[6][code][1]))
        else:
            cursor_res.execute(insert_sql, (code, codes[code], -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1))
    conn_res.commit()
    pass


def all_acti():
    table_name = 'allacti'
    scope = WAVE_SCOPE_THRESHOLD
    indent = 7
    stat_activity('allacti', True, scope, u_month_befor(indent*4), u_month_befor(0))
    # (waves > 30 and activity < 32) or (waves > 20 and activity < 30)
    conn = sqlite3.connect(PICKING_DB)
    cursor = conn.cursor()
    cursor.execute(
        "select distinct code from '%s' where activity < %s and waves > %s and avg_limit > %s order by activity" % (
            table_name, ACTIVITY_THRESHOLD, WAVE_COUNT_THRESHOLD, LIMIT_COUNT_THRESHOLD))

    u_write_to_file(u_create_path_by_system('t_acti_all.txt'), Fundamental.remove_st([row[0] for row in cursor]))
    cursor.execute(
        "select distinct code from '%s' where l_close_p > 1 and last_wave < 0 and activity < %s and waves > %s and avg_limit > %s and last_dif < 10 order by activity" % (
            table_name, ACTIVITY_THRESHOLD, WAVE_COUNT_THRESHOLD, LIMIT_COUNT_THRESHOLD))
    u_write_to_file(u_create_path_by_system('t_acti_today.txt'), Fundamental.remove_st([row[0] for row in cursor]))

    u_write_to_file(u_create_path_by_system('t_acti_lht.txt'), picking_get_lht())
    # get_lht()

def picking_get_lht():
    table_name = 'allacti'

    scope_threhold = 50
    conn = sqlite3.connect(PICKING_DB)
    cursor = conn.cursor()
    cursor.execute(
        "select  code,last_wave,l_close_p,last_dif,last_duration,sideway_duration,detail_dur,detail_scope from '%s' " % (table_name,))

    result = []
    for code,last_wave,l_close_p,last_dif,last_duration,sideway_duration,detail_dur,detail_scope in cursor:
        durs = detail_dur.split()
        scopes = detail_scope.split()
        if len(durs) < 2:
            continue
        dur1 = int(durs[1])
        dur2 = int(durs[0])
        scope1 = float(scopes[1])
        scope2 = float(scopes[0])

        if (scope1 > scope_threhold and dur2 < 10) or (scope2>scope_threhold and last_dif<-5):
            result.append(code)

    return result




def jx_acti():
    table_name = 'jxacti'
    scope = WAVE_SCOPE_THRESHOLD
    indent = 7
    # stat_activity(table_name, True, scope, '20170101', '20170630')
    stat_activity(table_name, True, scope, u_month_befor(indent*4), u_month_befor(indent*3))
    stat_activity(table_name, False, scope, u_month_befor(indent*3), u_month_befor(indent*2))
    stat_activity(table_name, False, scope, u_month_befor(indent*2), u_month_befor(indent*1))
    stat_activity(table_name, False, scope, u_month_befor(indent*1), u_month_befor(indent*0))
    analyze_activity(table_name, ACTIVITY_THRESHOLD, LIMIT_COUNT_THRESHOLD, 3)


def check_acti():
    table_name = 'allacti'
    last_wave = 28
    last_dif = 10
    overflow = 2.8
    concepts = u_read_input('data/hot_concepts')
    concepts_codes = Fundamental.get_codes_by_concept(concepts)

    conn = sqlite3.connect(PICKING_DB)
    cursor = conn.cursor()
    cursor.execute(
        "select distinct code from '%s' where last_wave < %s and activity < %s and waves > %s and avg_limit > %s and last_dif < %s" % (
            table_name, last_wave, ACTIVITY_THRESHOLD, WAVE_COUNT_THRESHOLD, LIMIT_COUNT_THRESHOLD, last_dif))

    codes = [row[0] for row in cursor if row[0] in concepts_codes]
    codes = Fundamental.remove_st(codes)
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
        u_write_to_file(u_create_path_by_system('t_acti_check.txt'), results)
        return results
    return {}


def check_attention():
    overflow = 2.8
    # concepts = u_read_input('data/hot_concepts')
    # concepts_codes = Fundamental.get_codes_by_concept(concepts)
    codes1 = u_read_input('data/attention.txt')
    codes2 = u_read_input('data/selection.txt')

    codes = Fundamental.name_to_codes(set(codes1+codes2))
    codes = Fundamental.remove_st(codes)
    # codes = [e for e in codes if e in concepts_codes]
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
        u_write_to_file(u_create_path_by_system('t_attention_check.txt'), results)
        return results
    return {}


def picking_daily():
    all_acti()
    jx_acti()
    stat_index([3.0, 5.0, 7.0, 9.0], '20140101', u_month_befor(0))
    create_index_info()
    calc_rps()


def create_index_info():
    with open('data/index_info.txt', 'w') as wfile:
        names = ['上证综指',
                 '深证成指',
                 '创业板指',
                 '中小板指',
                 '上证50',
                 '中证100',
                 '沪深300', ]
        conn = sqlite3.connect(PICKING_DB)
        cursor = conn.cursor()
        for name in names:
            wfile.write('%s：' % name)

            wfile.write(DataHandler.get_index_tech_index_info(name))
            query_sql = '''select scope,date_range,activity,max_up,avg_up,min_down,avg_down,
            last_wave,high_dif,low_dif,last_dif,last_duration,sideway_duration,detail_dur,detail_scope 
            from 'index' where name = '%s'  ''' % name
            cursor.execute(query_sql)
            for (
                    scope, date_range, activity, max_up, avg_up, min_down, avg_down, last_wave, high_dif, low_dif,
                    last_dif,
                    last_duration, sideway_duration, detail_dur, detail_scope) in cursor:
                wave_info = '''
    幅度为%s%%的波段统计(%s):
    %s
    %s
        波段平均持续 %s 天     波段最大涨幅 %s%%   平均波段涨幅 %s%%    波段最大跌幅 %s%%    波段平均跌幅 %s%%
        当前波段幅度 %s%%    已持续 %s 天   波段极值点距今有 %s 天
        当前股价比最高点跌幅 %s%%   比最低点涨幅 %s%%    比当前波段极值点涨跌幅 %s%%''' % (
                    scope, date_range, detail_scope, detail_dur, activity, max_up, avg_up, min_down, avg_down,
                    last_wave, last_duration, sideway_duration,
                    high_dif, low_dif, last_dif)

                wfile.write(wave_info)
            wfile.write('\n\n')

    pass


def get_picking_info(code):
    conn = sqlite3.connect(PICKING_DB)
    cursor = conn.cursor()
    cursor.execute('''
            select rps5,r5,rps10,r10,rps20,r20,rps30,r30,rps60,r60,rps120,r120,rps250,r250 from rps 
            where code = '%s'
        ''' % code)
    rps = [row for row in cursor]
    rps_info = 'rps统计:  无'
    if rps:
        (rps5, r5, rps10, r10, rps20, r20, rps30, r30, rps60, r60, rps120, r120, rps250, r250) = rps[0]
        rps_info = '''rps统计:
    5日涨幅   %8.3f%%   rps5:   %8.3f               10日涨幅  %8.3f%%  rps10: %8.3f  
    20日涨幅  %8.3f%%   rps20:  %8.3f               30日涨幅  %8.3f%%  rps30: %8.3f   
    60日涨幅  %8.3f%%   rps60:  %8.3f               120日涨幅 %8.3f%%  rps120:%8.3f  
    250日涨幅 %8.3f%%   rps250: %8.3f''' % (
            r5, rps5, r10, rps10, r20, rps20, r30, rps30, r60, rps60, r120, rps120, r250, rps250)
        # print(rps_info)

    wave_info = '波段信息：  无'
    cursor.execute('''
            select date_range,activity,avg_limit,max_up,avg_up,min_down,avg_down,last_wave,high_dif,low_dif,last_dif,last_duration,sideway_duration,detail_dur,detail_scope from allacti 
            where code = '%s'
        ''' % code)
    wave = [row for row in cursor]
    if wave:
        (date_range, activity, avg_limit, max_up, avg_up, min_down, avg_down, last_wave, high_dif, low_dif, last_dif,
         last_duration, sideway_duration, detail_dur, detail_scope) = wave[0]
        wave_info = '''波段统计(%s):
%s
%s
    波段平均持续 %s 天   涨停可能性 %s    波段最大涨幅 %s%%   平均波段涨幅 %s%%    波段最大跌幅 %s%%    波段平均跌幅 %s%%
    当前波段幅度 %s%%    已持续 %s 天   波段极值点距今有 %s 天
    当前股价比最高点跌幅 %s%%   比最低点涨幅 %s%%    比当前波段极值点涨跌幅 %s%%''' % (
            date_range, detail_scope, detail_dur, activity, avg_limit, max_up, avg_up, min_down, avg_down,
            last_wave, last_duration, sideway_duration,
            high_dif, low_dif, last_dif)
    ret_info = '%s\n%s' % (rps_info, wave_info)
    # print(ret_info)
    return ret_info


if __name__ == '__main__':
    import cProfile

    #
    # stat_index([3.0, 5.0, 7.0, 9.0], '20140101', u_month_befor(0))
    # create_index_info()
    # picking_daily()
    # all_acti()
    # jx_acti()
    # calc_rps()
    # verify('600086',18,'20170101', u_month_befor(0))
    # get_picking_info('600086')
    # get_lht()
    pass
