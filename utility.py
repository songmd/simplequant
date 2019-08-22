import sqlite3

import datetime
from dateutil.relativedelta import relativedelta


def u_format_list(ls):
    return ','.join(["'%s'" % e for e in ls])


# def u_save_bk_result(res):
#     conn = sqlite3.connect('bkresult.db')
#     cursor = conn.cursor()
#     cursor.execute(
#         "create table if not exists bkr (code TEXT,s_money REAL,e_money REAL,b_rate REAL,r_rate REAL,buy_time INT,win_time INT,w_rate REAL,datetime timestamp not null default(datetime('now', 'localtime')), b_date TEXT,e_date TEXT,detail TEXT)")
#
#     cursor.execute(
#         "INSERT INTO bkr (code,s_money,e_money,detail,b_date,e_date,b_rate,r_rate,buy_time,win_time,w_rate) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
#         (res['code'], round(res['s_money'], 2), round(res['e_money'], 2), res['detail'], res['b_date'], res['e_date'],
#          round(res['b_rate'] * 100, 3),
#          round(res['r_rate'] * 100, 3),
#          res['buy_time'], res['win_time'],
#          round(res['w_rate'] * 100, 3)))
#     conn.commit()
#
#
# def u_alter_bk_result_table_name(table):
#     conn = sqlite3.connect('bkresult.db')
#     cursor = conn.cursor()
#     cursor.execute("drop table if exists '%s'" % table)
#     cursor.execute("alter table 'bkr' rename to '%s'" % table)
#     conn.commit()
#
#
# def u_analyze_bk_result():
#     conn = sqlite3.connect('bkresult.db')
#     cursor = conn.cursor()
#     cursor.execute("select sum(buy_time),sum(win_time),sum(s_money),sum(e_money) from bkr")
#     for row in cursor:
#         r_rate = round((row[3] / row[2] - 1) * 100, 3)
#         w_rate = round(row[1] * 100 / row[0], 3)
#         print("buy_time:%s  r_rate:%s   w_rate:%s" % (row[0], r_rate, w_rate))


def u_month_befor(m):
    d = datetime.datetime.today()
    d -= relativedelta(months=m)
    return d.strftime('%Y%m%d')


def u_day_befor(d):
    day = datetime.datetime.today()
    day -= datetime.timedelta(days=d)
    return day.strftime('%Y%m%d')


def u_week_contain(day):
    dt = datetime.datetime.strptime(day, '%Y%m%d').date()
    next_week = dt + datetime.timedelta(days=7 - dt.weekday())
    return next_week.strftime('%Y%m%d')


def u_month_contain(day):
    dt = datetime.datetime.strptime(day, '%Y%m%d').date()
    dt -= datetime.timedelta(days=dt.day - 1)
    next_month = dt + relativedelta(months=1)
    return next_month.strftime('%Y%m%d')


def u_quarter_contain(day):
    dt = datetime.datetime.strptime(day, '%Y%m%d').date()
    dt -= datetime.timedelta(days=dt.day - 1)
    m = 3 - (dt.month - 1) % 3
    next_quarter = dt + relativedelta(months=m)
    return next_quarter.strftime('%Y%m%d')


def u_year_contain(day):
    dt = datetime.datetime.strptime(day, '%Y%m%d').date()
    dt = datetime.date(dt.year, 1, 1)
    next_year = dt + relativedelta(years=1)
    return next_year.strftime('%Y%m%d')


def u_half_year_contain(day):
    dt = datetime.datetime.strptime(day, '%Y%m%d').date()

    now = datetime.datetime.now()
    if now.month < 10 and dt.year == now.year and dt.month <= 6:
        dt = now.date()
    elif dt.month > 6:
        dt = datetime.date(dt.year + 1, 1, 1)
    else:
        dt = datetime.date(dt.year, 7, 1)

    return dt.strftime('%Y%m%d')


def u_read_input(file_name='input.txt'):
    with open(file_name) as input:
        lines = []
        while True:
            line = input.readline()
            if line:
                line = line.strip()
                if line and (line not in lines):
                    lines.append(line.replace('SH','').replace('SZ',''))
            else:
                break
        return lines


def u_write_to_file(file_name, ds):
    with open(file_name, 'w') as wfile:
        for e in ds:
            wfile.writelines(str(e) + '\n')


def u_calc_ratio(pre, now):
    return round((now / pre - 1) * 100, 2)


if __name__ == '__main__':
    #   import cProfile

    # cProfile.run('profile_run2()')
    u_read_input()
