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
    import itchat
    itchat.auto_login(hotReload=True)
    groups = itchat.get_chatrooms(update=True)
    target_group = ''
    nickname = '三语股票测试' if debug else '三语股票'
    for group in groups:
        if group['NickName'] == nickname:
            target_group = group['UserName']
            break

    def notify_fu(type, result, already_notify):
        need_notify = set(result) - already_notify
        already_notify |= set(result)
        if need_notify:
            rtm_save_item({e: result[e] for e in need_notify}, type)
            msg = '%s tagrgets:\n%s' % (type, ' '.join(need_notify))

            tempname = 'notify_%s.txt' % random.randint(1, 10)
            temppath = 'data/temp/%s' % tempname
            u_write_to_file(temppath, need_notify)
            u_write_to_file('data/all_notify.txt', already_notify)
            synthesis('data/all_notify.txt')
            synthesis(temppath)
            itchat.send(msg, toUserName=target_group)
            u_itchat_send_file('data/dgn/dgn_%s' % tempname, toUserName=target_group)

    while True:
        now = datetime.datetime.now()
        if now.hour >= 15:
            break
        if (now.hour == 11 and now.minute >= 30) or (now.hour >= 12 and now.hour < 13):
            time.sleep(5)
            continue

        result = check_acti()
        if result:
            synthesis('/Users/hero101/Documents/t_acti_check.txt')
        notify_fu('acti', result, already_notify)

        result = dh_check_macd()
        if result:
            synthesis('/Users/hero101/Documents/t_macd_check.txt')
        notify_fu('macd', result, already_notify)

        result = Fundamental.check_doctor()
        if result:
            synthesis('/Users/hero101/Documents/t_doctor_check.txt')
        notify_fu('doctor', result, already_notify)

        result = check_attention()
        if result:
            synthesis('/Users/hero101/Documents/t_attention_check.txt')
        notify_fu('attention', result, already_notify)

        time.sleep(5)

    u_write_to_file('/Users/hero101/Documents/t_notified.txt', already_notify)
    synthesis('/Users/hero101/Documents/t_notified.txt')
    u_itchat_send_file('data/dgn/dgn_t_notified.txt', toUserName=target_group)


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


if __name__ == '__main__':
    import cProfile

    # cProfile.run('real_time_monitor()')
    # rtm(True)
    rtm()
    # test()
    pass
