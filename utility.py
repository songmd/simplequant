import sqlite3
import os
import platform
import datetime
from dateutil.relativedelta import relativedelta

import requests


def u_get_proxy():
    return requests.get("http://127.0.0.1:5010/get/").json()


def u_format_list(ls):
    return ','.join(["'%s'" % e for e in ls])


def u_mk_dir(dir):
    if not os.path.isdir(dir):
        os.mkdir(dir)


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

def u_get_realtime_price(targets):
    from mytushare import get_realtime_quotes
    if targets:
        result = {}
        df = get_realtime_quotes([key for key in targets])
        for index, row in df.iterrows():
            code = row['code']
            result[code] = float(row['price'])
        return result
    return {}


def u_month_befor(m):
    d = datetime.datetime.today()
    d -= relativedelta(months=m)
    return d.strftime('%Y%m%d')


def u_day_befor(d):
    day = datetime.datetime.today()
    day -= datetime.timedelta(days=d)
    return day.strftime('%Y%m%d')


def u_time_now():
    now = datetime.datetime.now()

    return now.strftime('%H:%M:%S')


def u_days_diff(start_day, end_day):
    if not start_day:
        return 0
    s_dt = datetime.datetime.strptime(start_day, '%Y%m%d')
    e_dt = datetime.datetime.strptime(end_day, '%Y%m%d')

    diff = e_dt - s_dt
    return diff.days


def u_time_now_filename():
    now = datetime.datetime.now()

    return now.strftime('%H%M%S')


def u_week_contain(day):
    dt = datetime.datetime.strptime(day, '%Y%m%d').date()
    next_week = dt + datetime.timedelta(days=7 - dt.weekday())
    return next_week.strftime('%Y%m%d')


def u_week_begin(day=''):
    if day:
        dt = datetime.datetime.strptime(day, '%Y%m%d').date()
    else:
        dt = datetime.datetime.now()
    week_begin = dt + datetime.timedelta(days=-dt.weekday())
    return week_begin.strftime('%Y%m%d')


def u_month_contain(day):
    dt = datetime.datetime.strptime(day, '%Y%m%d').date()
    dt -= datetime.timedelta(days=dt.day - 1)
    next_month = dt + relativedelta(months=1)
    return next_month.strftime('%Y%m%d')


def u_month_after(day, m):
    dt = datetime.datetime.strptime(day, '%Y%m%d').date()
    dt += relativedelta(months=m)
    return dt.strftime('%Y%m%d')




def u_month_begin(day=''):
    if day:
        dt = datetime.datetime.strptime(day, '%Y%m%d').date()
    else:
        dt = datetime.datetime.now()
    dt -= datetime.timedelta(days=dt.day - 1)
    return dt.strftime('%Y%m%d')


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


def u_year_begin(day=''):
    if day:
        dt = datetime.datetime.strptime(day, '%Y%m%d').date()
    else:
        dt = datetime.datetime.now()
    dt = datetime.date(dt.year, 1, 1)
    return dt.strftime('%Y%m%d')


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
    with open(file_name,encoding='utf-8') as input:
        lines = []
        while True:
            line = input.readline()
            if line:
                line = line.strip()
                if line and (line not in lines):
                    lines.append(line.replace('SH', '').replace('SZ', ''))
            else:
                break
        return lines


def u_write_to_file(file_name, ds):
    with open(file_name, 'w',encoding='utf-8') as wfile:
        for e in ds:
            wfile.writelines(str(e) + '\n')


def u_file_intersection(files):
    if not files:
        return []
    ret = set(u_read_input(files[0]))
    for file in files[1:]:
        ret &= set(u_read_input(file))
    return ret


def u_write_to_file_append(file_name, ds):
    exist_lines = u_read_input(file_name)
    with open(file_name, 'a',encoding='utf-8') as wfile:
        for e in ds:
            if e not in exist_lines:
                wfile.writelines('\n' + str(e))


def u_calc_ratio(pre, now):
    return round((now / pre - 1) * 100, 2)


def u_itchat_send_file(file, toUserName=None):
    import itchat
    count = 0
    while count <= 3:
        try:
            itchat.send_file(file, toUserName=toUserName)
        except:
            count += 1
        else:
            break

def u_get_download_path():
    plat = platform.system()
    if plat == 'Windows':
        return 'C:/Users/songm/Downloads'
    elif plat == 'Darwin':
        return '/Users/hero101/Downloads'
    elif plat == 'Linux':
        return ''
    return ''

def u_create_path_by_system(name):
    plat = platform.system()
    if plat == 'Windows':
        return 'C:/Users/songm/Documents/simplequant/' + name
    elif plat == 'Darwin':
        return '/Users/hero101/Documents/' + name
    elif plat == 'Linux':
        return name
    return name

def u_same_week(d1,d2):
    dt1 = datetime.datetime.strptime(d1, '%Y%m%d').date()
    week_begin = dt1 + datetime.timedelta(days=-dt1.weekday())
    week_end = dt1 + datetime.timedelta(days=6-dt1.weekday())
    dt2 = datetime.datetime.strptime(d2, '%Y%m%d').date()
    return dt2 >= week_begin and dt2 <= week_end

def u_same_month(d1,d2):
    dt1 = datetime.datetime.strptime(d1, '%Y%m%d').date()
    dt2 = datetime.datetime.strptime(d2, '%Y%m%d').date()
    month_begin = dt1 - datetime.timedelta(days=dt1.day - 1)
    next_month= month_begin + relativedelta(months=1)
    dt2 = datetime.datetime.strptime(d2, '%Y%m%d').date()
    return dt2 >= month_begin and dt2 < next_month

def u_same_year(d1,d2):
    dt1 = datetime.datetime.strptime(d1, '%Y%m%d').date()
    dt2 = datetime.datetime.strptime(d2, '%Y%m%d').date()
    return dt2.year == dt1.year


if __name__ == '__main__':
    #   import cProfile

    # cProfile.run('profile_run2()')
    # u_read_input()
    # print(u_days_diff('20200416','20200416'))
    # print(u_file_intersection(['data/input.txt', 'data/attention.txt']))
    # print(platform.system())
    # print(u_create_path_by_system('temp.txt'))
    print( u_same_month('20201213','20201031') )
    pass
