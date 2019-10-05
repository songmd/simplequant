import sqlite3
from utility import *
from dataprocess import DataHandler
import json
from fundamental import Fundamental


# 仓位管理,资金统计
class PositionMgr(object):
    ACCOUNT_INFO = 'data/account_info.txt'
    TRADING_RECORD = 'data/trading_record.txt'
    POSMGR_DB = 'data/position.db'
    TRDREC_TB = 'trading_record'
    TRDSTAT_TB = 'trading_stat'

    @staticmethod
    def get_account(name):
        with open(PositionMgr.ACCOUNT_INFO) as f:
            accounts = json.load(f)
            # text = json.dumps(accounts, ensure_ascii=False, sort_keys=True, indent=4)
            # with open(PositionMgr.ACCOUNT_INFO, "w") as f:
            #     f.write(text)
            for ac in accounts:
                if ac['name'] == name:
                    return ac
            return None
        return None
        pass

    @staticmethod
    def update_trading_record():
        PositionMgr.backup()

        with open(PositionMgr.TRADING_RECORD) as f:
            trading_records = json.load(f)
            conn = sqlite3.connect(PositionMgr.POSMGR_DB)
            cursor = conn.cursor()
            table_name = PositionMgr.TRDREC_TB
            cursor.execute(
                '''create table if not exists '%s' (
                                        name TEXT,
                                        stock_code TEXT,
                                        type TEXT,
                                        trading_date TEXT,
                                        count INT,
                                        price real)'''
                % table_name)
            insert_sql = "insert into '%s' values (?,?,?,?,?,?)" % table_name
            for record in trading_records:
                cursor.execute(insert_sql,
                               (record['name'],
                                record['stock_code'],
                                record['type'],
                                record['trading_date'] if ('trading_date' in record and record['trading_date'])
                                else u_day_befor(0),
                                record['count'],
                                record['price']))
            conn.commit()

    @staticmethod
    def buy_cost(trading_commission, amount):
        # 佣金最低5元
        yongjin = round(max(trading_commission * amount, 5), 2)
        # 过户最低1元
        guohu = round(max(2e-05 * amount, 1), 2)

        return round(yongjin + guohu, 2)

    @staticmethod
    def sell_cost(trading_commission, amount):

        # 佣金最低5元
        yongjin = round(max(trading_commission * amount, 5), 2)
        # 过户最低1元
        guohu = round(max(2e-05 * amount, 1), 2)

        yinhua = round(max(1e-03 * amount, 1), 2)

        return round(yongjin + guohu + yinhua, 2)

    @staticmethod
    def get_trading_record(name):
        conn = sqlite3.connect(PositionMgr.POSMGR_DB)
        cursor = conn.cursor()
        table_name = PositionMgr.TRDREC_TB
        cursor.execute(
            "select * from '%s' where name = '%s' order by trading_date" % (table_name, name))
        return [{'name': row[0],
                 'stock_code': row[1],
                 'type': row[2],
                 'trading_date': row[3],
                 'count': row[4],
                 'price': row[5]} for row in cursor]
        pass

    @staticmethod
    def analyze_trading_records(trading_commission, records, cut_off_day):
        profit_count = 0
        loss_count = 0
        profit_amount = 0
        loss_amount = 0

        # 已完成交易
        done_trad_rec = []
        # 未完成交易
        undone_trad_rec = {}
        for rc in records:
            if rc['trading_date'] > cut_off_day:
                break
            # count == 0 and  stock_code 为空，说明是融资利息 或者 其他
            if rc['count'] == 0 and rc['stock_code'] == '':
                continue

            if rc['stock_code'] in undone_trad_rec:

                stock_count = 0
                last_date = ''
                for e in undone_trad_rec[rc['stock_code']]:
                    last_date = e['trading_date']
                    stock_count += (e['count'] if e['type'] == 'buy' else -e['count'])

                # 一笔交易 已完成
                if stock_count == 0 and last_date != rc['trading_date']:
                    # 当前记录移入已完成交易，
                    done_trad_rec.append(undone_trad_rec[rc['stock_code']])
                    undone_trad_rec[rc['stock_code']] = []
                    undone_trad_rec[rc['stock_code']].append(rc)
                else:
                    undone_trad_rec[rc['stock_code']].append(rc)
            else:
                undone_trad_rec[rc['stock_code']] = []
                undone_trad_rec[rc['stock_code']].append(rc)

        # 统计已完成交易
        for rcs in done_trad_rec:
            prf = 0
            for rc in rcs:
                #  count == 0 代表直接分红
                if rc['count'] == 0:
                    prf += rc['price']
                    continue
                #  price == 0 代表直接送转股票，不影响利润
                if rc['price'] == 0:
                    continue
                # 买入，消耗利润
                if rc['type'] == 'buy':
                    # 总金额
                    amount = rc['price'] * rc['count']
                    buy_cost = PositionMgr.buy_cost(trading_commission, amount)
                    prf -= (amount + buy_cost)
                    continue
                # 卖出 获得利润
                if rc['type'] == 'sell':
                    amount = rc['price'] * rc['count']
                    sell_cost = PositionMgr.sell_cost(trading_commission, amount)
                    prf += (amount - sell_cost)
            if prf >= 0:
                profit_count += 1
                profit_amount += prf
            else:
                loss_count += 1
                loss_amount += prf

        # 统计未已完成交易
        for code in undone_trad_rec:
            prf = 0
            stock_count = 0
            for rc in undone_trad_rec[code]:
                stock_count += (rc['count'] if rc['type'] == 'buy' else -rc['count'])
                #  count == 0 代表直接分红
                if rc['count'] == 0:
                    prf += rc['price']
                    continue
                #  price == 0 代表直接送转股票，不影响利润
                if rc['price'] == 0:
                    continue
                # 买入，消耗利润
                if rc['type'] == 'buy':
                    # 总金额
                    amount = rc['price'] * rc['count']
                    buy_cost = PositionMgr.buy_cost(trading_commission, amount)
                    prf -= (amount + buy_cost)
                    continue
                # 卖出 获得利润
                if rc['type'] == 'sell':
                    amount = rc['price'] * rc['count']
                    sell_cost = PositionMgr.sell_cost(trading_commission, amount)
                    prf += (amount - sell_cost)
            if stock_count != 0:
                stock_prices = DataHandler.get_history_price([code], cut_off_day)
                prf += stock_prices[code] * stock_count

            if prf >= 0:
                profit_count += 1
                profit_amount += prf
            else:
                loss_count += 1
                loss_amount += prf
        return round(profit_count, 2), round(profit_amount, 2), round(loss_count, 2), round(loss_amount, 2)

    @staticmethod
    def stat_trading_records(acount, records, cut_off_day):

        # 计算总资本，以及初始总净值
        initial_capital = acount['initial_capital']
        net_worth = acount['initial_net_worth']

        for cc in acount['capital_changed']:
            if cc['date_changed'] <= cut_off_day:
                initial_capital += cc['amount']
                net_worth += cc['amount']
        # 操作次数
        oper_count = 0
        # 融资利息
        interest = 0
        # 计算当前的仓位，计算当前现金
        positions = {}
        # 现金或者负债
        cash = net_worth

        # 交易成本
        trading_cost = 0
        for rc in records:
            if rc['trading_date'] > cut_off_day:
                break
            # 计算仓位
            if rc['stock_code']:
                if rc['stock_code'] in positions:
                    positions[rc['stock_code']] += (rc['count'] if rc['type'] == 'buy' else -rc['count'])
                else:
                    positions[rc['stock_code']] = (rc['count'] if rc['type'] == 'buy' else -rc['count'])

            # 计算现金
            # count == 0 and price < 0 stock_code 为空，说明是融资利息 或者其它
            if rc['count'] == 0 and rc['stock_code'] == '':
                cash += rc['price']
                if rc['price'] < 0:
                    interest += (-rc['price'])
                continue

            #  count == 0 代表直接分红
            if rc['count'] == 0:
                cash += rc['price']
                continue
            #  price == 0 代表直接送转股票，不影响现金
            if rc['price'] == 0:
                continue
            # 买入，消耗现金
            if rc['type'] == 'buy':
                oper_count += 1
                # 总金额
                amount = rc['price'] * rc['count']
                buy_cost = PositionMgr.buy_cost(acount['trading_commission'], amount)
                trading_cost += buy_cost
                cash -= (amount + buy_cost)
                continue
            if rc['type'] == 'sell':
                oper_count += 1
                amount = rc['price'] * rc['count']
                sell_cost = PositionMgr.sell_cost(acount['trading_commission'], amount)
                cash += (amount - sell_cost)
                trading_cost += sell_cost
                continue

        # 市值
        market_value = 0
        # 计算当前仓位的股票市值,从而确定当前净值
        if positions:
            pos_price = DataHandler.get_history_price(positions, cut_off_day)
            for code in positions:
                market_value += positions[code] * pos_price[code]

        # 当前净值
        net_worth = round(market_value + cash, 2)

        # 当前现金
        cash = round(cash, 2)

        # 仓位比
        position_rate = round(100 * market_value / net_worth, 3)
        # 收益率
        return_rate = round(100 * (net_worth - initial_capital) / initial_capital, 2)

        return net_worth, cash, market_value, return_rate, position_rate, trading_cost, interest, oper_count

    @staticmethod
    def stat_position(name):

        account = PositionMgr.get_account(name)
        records = PositionMgr.get_trading_record(name)
        days = u_read_input(DataHandler.CAL_TXT)

        conn = sqlite3.connect(PositionMgr.POSMGR_DB)
        cursor = conn.cursor()
        table_name = PositionMgr.TRDSTAT_TB
        # cursor.execute("drop table if exists '%s'" % table_name)
        cursor.execute(
            '''create table if not exists '%s' (
                                    name TEXT,
                                    tdate TEXT,
                                    ntcp real,
                                    cash real,
                                    mv real,
                                    rr real,
                                    pr real,
                                    tcost real,
                                    itrst real,
                                    opc int,
                                    prfc int,
                                    prfa real,
                                    lsc int,
                                    lsa real
                                    )'''
            % table_name)
        # 已经统计的日期
        cursor.execute("select distinct tdate from '%s' where name = '%s'" % (table_name, name))
        done_days = [row[0] for row in cursor]

        insert_sql = "insert into '%s' values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)" % table_name

        days = [day for day in days if day >= account['date_created'] and day not in done_days]
        for day in days:
            (net_worth, cash, market_value,
             return_rate, position_rate, trading_cost, interest, oper_count) = PositionMgr.stat_trading_records(
                account,
                records, day)

            (profit_count, profit_amount,
             loss_count, loss_amount) = PositionMgr.analyze_trading_records(
                account['trading_commission'], records, day)
            cursor.execute(insert_sql, (name, day, net_worth, cash,
                                        market_value, return_rate, position_rate,
                                        trading_cost, interest, oper_count,
                                        profit_count, profit_amount,
                                        loss_count, loss_amount
                                        ))
        conn.commit()

    @staticmethod
    def report_position(name, begin_date, end_date, report_file_ext):
        conn = sqlite3.connect(PositionMgr.POSMGR_DB)
        cursor = conn.cursor()
        table_name = PositionMgr.TRDSTAT_TB
        cursor.execute("select * from '%s' where name='%s' and tdate>='%s' and tdate<='%s' " % (
            table_name, name, begin_date, end_date))

        report_rec = [row for row in cursor]

        cursor.execute("select * from '%s' where name='%s' and tdate<'%s' order by tdate desc limit 1" % (
            table_name, name, begin_date))
        base_rec = [row for row in cursor]

        # 如果为空,报告最后一天
        if not report_rec:
            cursor.execute("select * from '%s' where name='%s' order by tdate desc limit 2 " % (
                table_name, name))
            rec = [row for row in cursor]
            report_rec = rec[0:1]
            base_rec = rec[1:2]

        if base_rec:
            base_rec = base_rec[0]
        else:
            # 根据账户创建信息，构建基准记录
            ac = PositionMgr.get_account(name)
            net_worth = ac['initial_net_worth']
            date_created = ac['date_created']

            for cc in ac['capital_changed']:
                if cc['date_changed'] <= date_created:
                    net_worth += cc['amount']
            base_rec = (name, date_created, net_worth, net_worth, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

        end_rec = report_rec[-1]

        # 净值变化
        dif_ntcp = round(end_rec[2] - base_rec[2], 2)

        # 收益率
        rr = round((end_rec[2] / base_rec[2] - 1) * 100, 2)

        # 指数基准收益率
        base_inds = DataHandler.get_index_base(base_rec[1], report_rec[-1][1])

        # 平均市值
        avg_mv = round(sum([e[4] for e in report_rec]) / len(report_rec), 2)

        # 平均仓位
        avg_pr = round(sum([e[6] for e in report_rec]) / len(report_rec), 2)

        # 交易成本
        dif_tcost = end_rec[7] - base_rec[7]

        # 融资利息
        dif_itrst = end_rec[8] - base_rec[8]

        # 操作次数
        dif_opc = end_rec[9] - base_rec[9]

        # 盈利次数
        dif_prfc = end_rec[10] - base_rec[10]

        # 盈利总额
        dif_prfa = end_rec[11] - base_rec[11]

        # 亏损次数
        dif_lsc = end_rec[12] - base_rec[12]

        # 亏损总额
        dif_lsa = end_rec[13] - base_rec[13]

        base_inds_info = ['%s:       %s %%' % (k, base_inds[k]) for k in base_inds]

        info = '''账户:   %s         统计日期:   %s-%s
        
净值变化:      %8s 元

账户收益率:     %s%%
%s

平均市值：       %8s 元           平均仓位:   %8s %%
交易成本:        %8s 元           融资利息:   %8s 元         
操作次数:        %8s
盈利次数:        %8s              盈利总额:   %8s 元    
亏损次数:        %8s              亏损总额:   %8s 元''' % (
            name, base_rec[1], end_rec[1], dif_ntcp, rr, '\n'.join(base_inds_info), avg_mv, avg_pr,
            dif_tcost, dif_itrst, dif_opc, dif_prfc, dif_prfa, dif_lsc, dif_lsa
        )
        with open('data/reports/%s_%s.txt' % (name, report_file_ext), 'w') as wfile:
            wfile.write(info)
        pass

    @staticmethod
    def report_position_last_day(name):
        conn = sqlite3.connect(PositionMgr.POSMGR_DB)
        cursor = conn.cursor()
        table_name = PositionMgr.TRDSTAT_TB
        cursor.execute("select * from '%s' where name='%s' order by tdate desc limit 2 " % (
            table_name, name))
        rec = [row for row in cursor]
        report_rec = rec[0:1]
        base_rec = rec[1:2]
        if base_rec:
            base_rec = base_rec[0]
        else:
            # 根据账户创建信息，构建基准记录
            ac = PositionMgr.get_account(name)
            net_worth = ac['initial_net_worth']
            date_created = ac['date_created']

            for cc in ac['capital_changed']:
                if cc['date_changed'] <= date_created:
                    net_worth += cc['amount']
            base_rec = (name, date_created, net_worth, net_worth, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        end_rec = report_rec[-1]
        # 当前净值
        ntcp = end_rec[2]
        # 净值变化
        dif_ntcp = round(end_rec[2] - base_rec[2], 2)
        # 收益率
        rr = round((end_rec[2] / base_rec[2] - 1) * 100, 2)
        # 当前总市值
        tmv = end_rec[4]
        # 当前仓位
        tpr = end_rec[6]
        # 当前可用现金
        cash = end_rec[3]
        # 指数基准收益率
        base_inds = DataHandler.get_index_base(base_rec[1], report_rec[-1][1])
        base_inds_info = ['%s:       %s %%' % (k, base_inds[k]) for k in base_inds]
        pos_infos = []
        records = PositionMgr.get_trading_record(name)
        # 计算当前的仓位
        positions = {}
        for rc in records:
            # 计算仓位
            if rc['stock_code']:
                if rc['stock_code'] in positions:
                    positions[rc['stock_code']] += (rc['count'] if rc['type'] == 'buy' else -rc['count'])
                else:
                    positions[rc['stock_code']] = (rc['count'] if rc['type'] == 'buy' else -rc['count'])
        if positions:
            pos_price = DataHandler.get_history_price(positions, u_day_befor(0))
            code2name = Fundamental.get_codes()
            for code in positions:
                if positions[code] != 0:
                    mv = positions[code] * pos_price[code]
                    pr = round(mv * 100 / ntcp, 2)
                    pos_infos.append('%-8s        市值:    %8s元     仓位占比   %s%%' % (code2name[code], mv, pr))

        info = '''账户:   %s        
净值变化:       %8s 元           
账户收益率:       %s%%
%s

当前净值:        %8s  元
当前市值：       %8s  元           
可用现金:        %8s  元
当前仓位:        %8s %%             
%s''' % (
            name, dif_ntcp, rr,
            '\n'.join(base_inds_info),
            ntcp, tmv, cash, tpr,
            '\n'.join(pos_infos),
        )
        with open('data/reports/%s_today.txt' % (name,), 'w') as wfile:
            wfile.write(info)
        pass

    @staticmethod
    def trade(name, type, stock, count, price, date_=''):
        names = Fundamental.get_names()
        if stock in names:
            stock = names[stock]
        PositionMgr.backup()
        conn = sqlite3.connect(PositionMgr.POSMGR_DB)
        cursor = conn.cursor()
        table_name = PositionMgr.TRDREC_TB
        cursor.execute(
            '''create table if not exists '%s' (
                                    name TEXT,
                                    stock_code TEXT,
                                    type TEXT,
                                    trading_date TEXT,
                                    count INT,
                                    price real)'''
            % table_name)
        insert_sql = "insert into '%s' values (?,?,?,?,?,?)" % table_name
        if not date_:
            date_ = u_day_befor(0)
        cursor.execute(insert_sql,
                       (name, stock, type, date_, count, price))

        conn.commit()

    @staticmethod
    def daily_run():
        PositionMgr.stat_position('刘波')
        PositionMgr.stat_position('宋茂东')

        PositionMgr.report_position_last_day('刘波')
        PositionMgr.report_position_last_day('宋茂东')

        PositionMgr.export_position()

        PositionMgr.report_position('刘波', u_day_befor(0), u_day_befor(0), 'd')
        PositionMgr.report_position('宋茂东', u_day_befor(0), u_day_befor(0), 'd')

        PositionMgr.report_position('刘波', u_week_begin(), u_day_befor(0), 'w')
        PositionMgr.report_position('宋茂东', u_week_begin(), u_day_befor(0), 'w')

        PositionMgr.report_position('刘波', u_month_begin(), u_day_befor(0), 'm')
        PositionMgr.report_position('宋茂东', u_month_begin(), u_day_befor(0), 'm')

        PositionMgr.report_position('刘波', u_year_begin(), u_day_befor(0), 'y')
        PositionMgr.report_position('宋茂东', u_year_begin(), u_day_befor(0), 'y')
        pass

    @staticmethod
    def backup():
        import shutil
        import datetime
        now = datetime.datetime.now()
        file_path = '/Users/hero101/backups/simplequant/position_%s.db' % now.strftime('%Y%m%d%H')
        shutil.copyfile(PositionMgr.POSMGR_DB, file_path)
        pass

    @staticmethod
    def do_trading():
        PositionMgr.backup()
        PositionMgr.trade('刘波', 'buy', '创维数字', 24500, 8.58, '20190930')
        PositionMgr.trade('刘波', 'buy', '光韵达', 11700, 11.380, '20190930')
        PositionMgr.trade('刘波', 'buy', '涪陵榨菜', 7100, 22.33, '20190930')
        PositionMgr.trade('刘波', 'buy', '星云股份', 6400, 16.68, '20190930')
        PositionMgr.trade('刘波', '', '', 0, 133.9, '20190930')

        PositionMgr.trade('宋茂东', 'buy', '创维数字', 34500, 8.58, '20190930')
        PositionMgr.trade('宋茂东', 'buy', '中贝通信', 7000, 26.50, '20190930')
        PositionMgr.trade('宋茂东', 'buy', '天奥电子', 4500, 30.60, '20190930')
        PositionMgr.trade('宋茂东', '', '', 0, 136.22, '20190930')

    @staticmethod
    def export_position():
        def export_position_fu(name):
            records = PositionMgr.get_trading_record(name)
            # 计算当前的仓位
            positions = {}
            for rc in records:
                # 计算仓位
                if rc['stock_code']:
                    if rc['stock_code'] in positions:
                        positions[rc['stock_code']] += (rc['count'] if rc['type'] == 'buy' else -rc['count'])
                    else:
                        positions[rc['stock_code']] = (rc['count'] if rc['type'] == 'buy' else -rc['count'])

            code2name = Fundamental.get_codes()
            p1 = set()
            p2 = set()
            for code in positions:
                if positions[code] != 0:
                    p1.add(code2name[code])
                else:
                    p2.add(code2name[code])
            return p1, p2

        lp1, lp2 = export_position_fu('刘波')
        sp1, sp2 = export_position_fu('宋茂东')
        u_write_to_file('data/position.txt', lp1 | sp1)
        u_write_to_file('data/position_once.txt', lp2 | sp2)


if __name__ == '__main__':
    import cProfile

    # PositionMgr.do_trading()
    # PositionMgr.update_trading_record()
    PositionMgr.daily_run()
    # PositionMgr.report_position('刘波', '20180101', '20191001')
    # PositionMgr.report_position('宋茂东', '20180101', '20191001')
