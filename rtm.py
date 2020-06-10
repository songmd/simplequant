from dataprocess import dh_check_macd
from picking import check_acti, check_attention
from synthesis import synthesis
from fundamental import Fundamental
from utility import *
import random
import time

RTM_DB = 'data/rtm.db'
RTM_TB = 'rtm'


def rtm_save_item(items, type):
    conn = sqlite3.connect(RTM_DB)
    cursor = conn.cursor()
    table_name = RTM_TB
    cursor.execute(
        '''create table if not exists '%s' (
        code  TEXT,
        date_ TEXT,
        time_ TEXT,
        price real,
        type_ TEXT
        )''' % table_name)
    insert_sql = "insert into '%s' values (?,?,?,?,?)" % table_name
    for key in items:
        cursor.execute(insert_sql, (key, u_day_befor(0), u_time_now(), items[key], type))
    conn.commit()


def rtm_get_already_notified():
    conn = sqlite3.connect(RTM_DB)
    cursor = conn.cursor()
    table_name = RTM_TB
    try:
        cursor.execute("select code from '%s' where date_='%s'" % (table_name, u_day_befor(0)))
        return set([row[0] for row in cursor])
    except:
        return set()


def rtm(debug=False):
    already_notify = rtm_get_already_notified()

    # import itchat
    # itchat.auto_login(hotReload=True)
    # groups = itchat.get_chatrooms(update=True)
    # target_group = ''
    # nickname = '三语股票测试'
    # # nickname = '三语股票测试' if debug else '三语股票'
    # for group in groups:
    #     if group['NickName'] == nickname:
    #         target_group = group['UserName']
    #         break

    def notify_fu(type, result, already_notify):
        need_notify = set(result) - already_notify
        already_notify |= set(result)
        if need_notify:
            rtm_save_item({e: result[e] for e in need_notify}, type)
            msg = '%s tagrgets:\n%s' % (type, ' '.join(need_notify))

            tempname = '%s_%s_%s.txt' % (u_time_now_filename(), random.randint(1, 5), type)
            temppath = 'data/%s' % u_day_befor(0)
            u_mk_dir(temppath)
            u_write_to_file('%s/%s' % (temppath, tempname), need_notify)
            all_notify = 'data/%s/all_notify.txt' % u_day_befor(0)
            u_write_to_file(all_notify, already_notify)
            synthesis(all_notify, u_day_befor(0))
            synthesis('%s/%s' % (temppath, tempname), u_day_befor(0))
            # itchat.send(msg, toUserName=target_group)
            # u_itchat_send_file('data/dgn/%s/dgn_%s' % (u_day_befor(0),tempname), toUserName=target_group)

    print('enter while')
    while True:

        result = check_attention()
        if result:
            synthesis('/Users/hero101/Documents/t_attention_check.txt')
        notify_fu('attention', result, already_notify)

        result = dh_check_macd()
        if result:
            synthesis('/Users/hero101/Documents/t_macd_check.txt')
        notify_fu('macd', result, already_notify)

        result = check_acti()
        if result:
            synthesis('/Users/hero101/Documents/t_acti_check.txt')
        notify_fu('acti', result, already_notify)

        result = Fundamental.check_doctor()
        if result:
            synthesis('/Users/hero101/Documents/t_doctor_check.txt')
        notify_fu('doctor', result, already_notify)

        result = Fundamental.check_yuzen()
        if result:
            synthesis('/Users/hero101/Documents/t_yuzen_check.txt')
        notify_fu('yuzen', result, already_notify)

        now = datetime.datetime.now()
        if now.hour >= 15:
            break
        if (now.hour == 11 and now.minute >= 30) or (now.hour >= 12 and now.hour < 13) or (
                now.hour == 9 and now.minute < 26):
            time.sleep(5)
        continue

    u_write_to_file('/Users/hero101/Documents/t_notified.txt', already_notify)
    synthesis('/Users/hero101/Documents/t_notified.txt')
    # u_itchat_send_file('data/dgn/dgn_t_notified.txt', toUserName=target_group)
    daily_stat_rtm()

    print('safe quit')


def stat_rtm(days):
    conn = sqlite3.connect(RTM_DB)
    cursor = conn.cursor()
    table_name = RTM_TB
    cursor.execute("select distinct date_ from '%s' order by date_ desc limit %s" % (table_name, days))
    dates = [row[0] for row in cursor]
    cursor.execute("select * from '%s' where date_ in (%s)" % (table_name, ','.join(['%s' % e for e in dates])))
    records = [row for row in cursor]
    code_prices = u_get_realtime_price({e[0] for e in records})
    code_names = Fundamental.get_codes()
    code_details = []

    types = set()
    result = []
    codes = []

    for rc in records:
        code = rc[0]
        codes.append(code)
        ntfy_price = rc[3]
        now_price = code_prices[rc[0]]
        range = round((now_price / ntfy_price - 1) * 100, 2)
        type = rc[4]
        code_details.append('%-6s  %s %s   %-10s  当时价格:%6s元     现价:%6s元   价格变化幅度:%6s%%' % (
            code_names[code], rc[1], rc[2], type, ntfy_price, now_price, range
        ))
        types.add(type)
        result.append({'type': type, 'range': range})

    types.add('')
    stat_detail = []
    for type in types:
        win = len([e for e in result if (type == '' or e['type'] == type) and e['range'] > 0])
        los = len([e for e in result if (type == '' or e['type'] == type) and e['range'] <= 0])
        avg_rr = round(sum([e['range'] for e in result if (type == '' or e['type'] == type)]) / (win + los), 2)
        win_r = round(win * 100 / (win + los), 2)

        stat_detail.append('%-10s  成功:%2s  次   失败:%2s  次   平均回报:%6s%%    胜率:%6s%%' % (
            type if type != '' else 'total', win, los, avg_rr, win_r
        ))
    info = '''%s
    
    
%s
''' % ('\n'.join(stat_detail), '\n'.join(code_details))
    with open('data/reports/rtm_%s.txt' % (days), 'w') as wfile:
        wfile.write(info)

    u_write_to_file('/Users/hero101/Documents/t_notified_%s.txt' % days, codes)
    # synthesis('/Users/hero101/Documents/t_notified_%s.txt' % days)
    pass


def test():
    import itchat
    itchat.auto_login(hotReload=True)
    groups = itchat.get_chatrooms(update=True)
    target_group = ''
    nickname = '三语股票测试'
    for group in groups:
        if group['NickName'] == nickname:
            target_group = group['UserName']
            break
    itchat.send_file('dgn_t_acti_check.txt', toUserName=target_group)


def daily_stat_rtm():
    stat_rtm(1)
    stat_rtm(2)
    stat_rtm(3)
    stat_rtm(4)
    stat_rtm(5)


def test2():
    import tushare as ts
    df = ts.get_tick_data('603106', date='2019-05-13', src='tt')
    print(df)


if __name__ == '__main__':
    import cProfile

    # cProfile.run('real_time_monitor()')
    # rtm(True)
    # daily_stat_rtm()
    rtm()
    # test2()

    pass
