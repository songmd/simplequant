from dataprocess import dh_check_macd
from picking import check_acti, check_attention
from synthesis import synthesis
from fundamental import Fundamental

import time


def rtm(debug=False):
    already_notify = set()
    import itchat
    itchat.auto_login(hotReload=True)
    groups = itchat.get_chatrooms(update=True)
    target_group = ''
    nickname = '三语股票测试' if debug else '三语股票'
    for group in groups:
        if group['NickName'] == nickname:
            target_group = group['UserName']
            break

    while True:
        result = set(check_acti())
        if result:
            synthesis('/Users/hero101/Documents/t_acti_check.txt')
        need_notify = result - already_notify
        already_notify |= result
        if need_notify:
            msg = 'check_acti targets:\n%s' % (' '.join(need_notify),)
            itchat.send(msg, toUserName=target_group)
            itchat.send_file('data/dgn_t_acti_check.txt', toUserName=target_group)

        result = set(dh_check_macd())
        if result:
            synthesis('/Users/hero101/Documents/t_macd_check.txt')
        need_notify = result - already_notify
        already_notify |= result
        if need_notify:
            msg = 'check_macd targets:\n%s' % (' '.join(need_notify),)
            itchat.send(msg, toUserName=target_group)
            itchat.send_file('data/dgn_t_macd_check.txt', toUserName=target_group)

        result = set(Fundamental.check_doctor())
        if result:
            synthesis('/Users/hero101/Documents/t_doctor_check.txt')
        need_notify = result - already_notify
        already_notify |= result
        if need_notify:
            msg = 'check_doctor targets:\n%s' % (' '.join(need_notify),)
            itchat.send(msg, toUserName=target_group)
            itchat.send_file('data/dgn_t_doctor_check.txt', toUserName=target_group)

        result = set(check_attention())
        if result:
            synthesis('/Users/hero101/Documents/t_attention_check.txt')
        need_notify = result - already_notify
        already_notify |= result
        if need_notify:
            msg = 'check_attention targets:\n%s' % (' '.join(need_notify),)
            itchat.send(msg, toUserName=target_group)
            itchat.send_file('data/dgn_t_attention_check.txt', toUserName=target_group)
        time.sleep(5)


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
