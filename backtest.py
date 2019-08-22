from dataprocess import DataHandler
from utility import *
from fundamental import Fundamental
BT_RESULT_DB = 'data/btresult.db'
BT_RESULT_TXT = 'data/btresult.txt'


#   回测指定策略
def run_backtest(strategy, target, begin_date, end_date):
    codes = []
    if not target:
        codes = DataHandler.get_codes()
    elif target[-4:] == '.txt':
        codes = u_read_input(target)
    else:
        codes.append(target)

    # 回测结果表
    res_table = strategy.result_table()
    insert_sql = "insert into '%s' values (?,?,?,?,?,?)" % res_table
    res_conn = sqlite3.connect(BT_RESULT_DB)
    res_cursor = res_conn.cursor()

    if strategy.need_record:
        res_cursor.execute("drop table if exists '%s'" % res_table)
        res_cursor.execute(
            "create table if not exists '%s' (code TEXT,b_price REAL,s_price REAL,b_date TEXT,s_date TEXT,rt_rate REAL)" % res_table)

    win_time = 0
    play_time = 0
    rt_rate = 0
    win_rate = 0
    # 回测源数据
    conn = sqlite3.connect(DataHandler.COOKECD_DB)
    cursor = conn.cursor()
    time_cond = " and trade_date>='%s' and trade_date<='%s'" % (begin_date, end_date)

    for code in codes:
        print(code)
        sql = "SELECT %s FROM daily WHERE code='%s'%s" % (','.join(strategy.query_column), code, time_cond)
        cursor.execute(sql)
        strategy.reset(code)
        for row in cursor:
            record = strategy.back_test(code, row)
            if record and strategy.need_record:
                play_time += 1
                if record[2] > record[1]:
                    win_time += 1
                srt_rate = round((record[2] / record[1] - 1) * 100, 3)
                rt_rate += srt_rate
                res_cursor.execute(insert_sql, record + (srt_rate,))

    if strategy.need_record:
        res_cursor.execute("create index 'isdc_%s' on '%s'(code,s_date);" % (res_table, res_table))
        res_conn.commit()

    with open(BT_RESULT_TXT, 'a') as wfile:
        if play_time > 0:
            win_rate = round(win_time * 100 / play_time, 3)
            rt_rate /= play_time
        rt = '%s  target:%s  data:%s-%s       rt_rate:%s   win_rate:%s  win_time:%s   play_time:%s' % (
            strategy.get_id(), target, begin_date, end_date, rt_rate, win_rate, win_time, play_time
        )
        print(rt)
        wfile.writelines(rt + '\n')


def analyze_backtest_by_time(table_name, begin_date, end_date, interval):
    print('being analyze bk by time interval=', interval, '....')
    conn = sqlite3.connect(BT_RESULT_DB)
    cursor = conn.cursor()

    # 回测分析表
    analyze_table = 'analyze_time_%s_%s' % (interval, table_name)
    insert_sql = "insert into '%s' values (?,?,?,?,?,?,?,?,?)" % analyze_table
    cursor.execute("drop table if exists '%s'" % analyze_table)
    cursor.execute(
        "create table if not exists '%s' (date_hz TEXT,win_time INT,loss_time INT,win_rate REAL,rt_rate REAL,max_win_rt REAL ,max_loss_rt REAL,avg_win_rt REAL,avg_loss_rt REAL)" % analyze_table)

    while True:
        interval_date = ''
        if interval == 7:
            interval_date = u_week_contain(begin_date)
        elif interval == 30:
            interval_date = u_month_contain(begin_date)
        elif interval == 90:
            interval_date = u_quarter_contain(begin_date)
        else:
            interval_date = u_year_contain(begin_date)
        selsql = "select * from '%s' where s_date>='%s' and s_date<'%s'" % (table_name, begin_date, interval_date)
        cursor.execute(selsql)
        win_time = 0
        loss_time = 0
        win_rate = 0
        max_rt_rate = 0
        min_rt_rate = 0
        avg_win_rt_rate = 0
        avg_loss_rt_rate = 0
        rt_rate = 0

        for row in cursor:
            if row[5] > 0:
                win_time += 1
                avg_win_rt_rate += row[5]
                if row[5] > max_rt_rate:
                    max_rt_rate = row[5]
            else:
                loss_time += 1
                if row[5] < min_rt_rate:
                    min_rt_rate = row[5]
                avg_loss_rt_rate += row[5]
        if win_time or loss_time:
            rt_rate = round((avg_loss_rt_rate + avg_win_rt_rate) / (win_time + loss_time), 3)
        if win_time:
            avg_win_rt_rate /= win_time
        if loss_time:
            avg_loss_rt_rate /= loss_time

        if win_time > 0:
            win_rate = round(win_time * 100 / (win_time + loss_time), 3)

        cursor.execute(insert_sql, (
            '%s-%s' % (begin_date, interval_date), win_time, loss_time, win_rate, rt_rate, max_rt_rate, min_rt_rate,
            round(avg_win_rt_rate, 3),
            round(avg_loss_rt_rate, 3)))
        conn.commit()
        if interval_date >= end_date:
            break
        begin_date = interval_date
    print('end analyze bk by time interval=', interval)


def analyze_backtest_by_code_time(table_name, begin_date, end_date, interval, output):
    print('begin analyze by code time')

    dates = []
    dates.append(begin_date)
    while True:
        interval_date = ''
        if interval == 7:
            interval_date = u_week_contain(begin_date)
        elif interval == 30:
            interval_date = u_month_contain(begin_date)
        elif interval == 90:
            interval_date = u_quarter_contain(begin_date)
        elif interval == 120:
            interval_date = u_half_year_contain(begin_date)
        else:
            interval_date = u_year_contain(begin_date)
        dates.append(interval_date)
        if interval_date >= end_date:
            break
        begin_date = interval_date

    conn = sqlite3.connect(BT_RESULT_DB)
    cursor = conn.cursor()
    cursor.execute(
        "select distinct code from '%s' where s_date>='%s' and s_date<='%s'" % (table_name, dates[0], dates[-1]))
    codes = [row[0] for row in cursor]

    # 回测分析表
    analyze_table = 'analyze_code_time_%s_%s' % (table_name, interval)
    insert_sql = "insert into '%s' values (?,?,?,?,?,?,?,?,?,?)" % analyze_table
    cursor.execute("drop table if exists '%s'" % analyze_table)
    cursor.execute(
        "create table if not exists '%s' (code TEXT,date_range TEXT,win_time INT,loss_time INT,win_rate REAL,rt_rate REAL,max_win_rt REAL ,max_loss_rt REAL,avg_win_rt REAL,avg_loss_rt REAL)" % analyze_table)

    date_intervals = len(dates) - 1
    output_code = []
    for code in codes:
        isok = True
        print('analyze code_time:%s' % code)
        for index in range(date_intervals):
            cursor.execute(
                "select * from '%s' where code = '%s' and s_date>='%s' and s_date<'%s'" % (
                    table_name, code, dates[index], dates[index + 1]))
            win_time = 0
            loss_time = 0
            win_rate = 0
            max_rt_rate = 0
            min_rt_rate = 0
            avg_win_rt_rate = 0
            avg_loss_rt_rate = 0
            rt_rate = 0

            for row in cursor:
                if row[5] > 0:
                    win_time += 1
                    avg_win_rt_rate += row[5]
                    if row[5] > max_rt_rate:
                        max_rt_rate = row[5]
                else:
                    loss_time += 1
                    if row[5] < min_rt_rate:
                        min_rt_rate = row[5]
                    avg_loss_rt_rate += row[5]
            if win_time or loss_time:
                rt_rate = round((avg_loss_rt_rate + avg_win_rt_rate) / (win_time + loss_time), 3)

            if win_time:
                avg_win_rt_rate /= win_time
            if loss_time:
                avg_loss_rt_rate /= loss_time

            if win_time > 0:
                win_rate = round(win_time * 100 / (win_time + loss_time), 3)

            if win_rate < 40:
                isok = False

            cursor.execute(insert_sql, (
                code, index, win_time, loss_time, win_rate, rt_rate, max_rt_rate,
                min_rt_rate, round(avg_win_rt_rate, 3),
                round(avg_loss_rt_rate, 3)))
            conn.commit()
        if isok:
            output_code.append(code)

    u_write_to_file(output, output_code)


def analyze_backtest_by_code(table_name, begin_date, end_date, output):
    print('begin analyze by code')
    codesql = "select distinct code from '%s'" % table_name
    selsql = "select * from '%s'" % table_name
    conds = []

    conds.append("s_date>='%s'" % begin_date)

    conds.append("s_date<='%s'" % end_date)

    codesql += " where " + ' and '.join(conds)

    conn = sqlite3.connect(BT_RESULT_DB)
    cursor = conn.cursor()
    cursor.execute(codesql)
    codes = []
    for row in cursor:
        codes.append(row[0])

    # 回测分析表
    analyze_table = 'analyze_code_%s' % table_name
    insert_sql = "insert into '%s' values (?,?,?,?,?,?,?,?,?)" % analyze_table
    cursor.execute("drop table if exists '%s'" % analyze_table)
    cursor.execute(
        "create table if not exists '%s' (code TEXT,win_time INT,loss_time INT,win_rate REAL,rt_rate REAL,max_win_rt REAL ,max_loss_rt REAL,avg_win_rt REAL,avg_loss_rt REAL)" % analyze_table)

    for code in codes:
        print('analyze code:%s' % code)
        codeconds = conds + ["code='%s'" % code]
        cursor.execute(selsql + " where " + ' and '.join(codeconds))
        win_time = 0
        loss_time = 0
        win_rate = 0
        max_rt_rate = 0
        min_rt_rate = 0
        avg_win_rt_rate = 0
        avg_loss_rt_rate = 0
        rt_rate = 0

        for row in cursor:
            if row[5] > 0:
                win_time += 1
                avg_win_rt_rate += row[5]
                if row[5] > max_rt_rate:
                    max_rt_rate = row[5]
            else:
                loss_time += 1
                if row[5] < min_rt_rate:
                    min_rt_rate = row[5]
                avg_loss_rt_rate += row[5]
        if win_time or loss_time:
            rt_rate = round((avg_loss_rt_rate + avg_win_rt_rate) / (win_time + loss_time), 3)
        if win_time:
            avg_win_rt_rate /= win_time
        if loss_time:
            avg_loss_rt_rate /= loss_time

        if win_time > 0:
            win_rate = round(win_time * 100 / (win_time + loss_time), 3)

        cursor.execute(insert_sql, (
            code, win_time, loss_time, win_rate, rt_rate, max_rt_rate, min_rt_rate, round(avg_win_rt_rate, 3),
            round(avg_loss_rt_rate, 3)))
        conn.commit()

    cursor.execute(
        "select code from '%s' where win_time >= 8 and win_rate > 50 and rt_rate > 0 order by win_rate desc" % analyze_table)
    u_write_to_file(output, Fundamental.remove_st([row[0] for row in cursor]))
