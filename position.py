import sqlite3
from utility import *
from dataprocess import DataHandler
import json
from fundamental import Fundamental
import math


# 仓位管理,资金统计
class PositionMgr(object):
    ACCOUNT_INFO = 'data/account_info.txt'
    TRADING_RECORD = 'data/trading_record.txt'
    POSMGR_DB = 'data/position.db'
    TRDREC_TB = 'trading_record'
    TRDSTAT_TB = 'trading_stat'
    FIANCE_TB = 'fiance_record'

    @staticmethod
    def get_account(name):
        with open(PositionMgr.ACCOUNT_INFO,encoding='utf-8') as f:
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

        with open(PositionMgr.TRADING_RECORD,encoding='utf-8') as f:
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
    def buy_cost(trading_commission, amount, code):
        # 佣金最低5元
        yongjin = round(max(trading_commission * amount, 5), 2)
        # 过户最低1元
        guohu = round(max(2e-05 * amount, 1), 2)
        if code[0] != '6':
            guohu = 0
        return round(yongjin + guohu, 2)

    @staticmethod
    def sell_cost(trading_commission, amount, code):

        # 佣金最低5元
        yongjin = round(max(trading_commission * amount, 5), 2)
        # 过户最低1元
        guohu = round(max(2e-05 * amount, 1), 2)

        if code[0] != '6':
            guohu = 0

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
                    buy_cost = PositionMgr.buy_cost(trading_commission, amount, rc['stock_code'])
                    prf -= (amount + buy_cost)
                    continue
                # 卖出 获得利润
                if rc['type'] == 'sell':
                    amount = rc['price'] * rc['count']
                    sell_cost = PositionMgr.sell_cost(trading_commission, amount, rc['stock_code'])
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
                    buy_cost = PositionMgr.buy_cost(trading_commission, amount, rc['stock_code'])
                    prf -= (amount + buy_cost)
                    continue
                # 卖出 获得利润
                if rc['type'] == 'sell':
                    amount = rc['price'] * rc['count']
                    sell_cost = PositionMgr.sell_cost(trading_commission, amount, rc['stock_code'])
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
    def get_realtime_price(positions):
        result = {}
        from mytushare import get_realtime_quotes
        df = get_realtime_quotes([key for key in positions])
        for index, row in df.iterrows():
            code = row['code']
            if code in positions:
                price = float(row['price'])
                result[code] = price

        return result

    @staticmethod
    def stat_trading_records(acount, records, cut_off_day, realtime=False):

        # 计算总资本，以及初始总净值
        initial_capital = acount['initial_capital']
        # # 利率
        # interest_rate = acount['interest_rate']

        net_worth = acount['initial_net_worth']

        for cc in acount['capital_changed']:
            if cc['date_changed'] <= cut_off_day:
                initial_capital += cc['amount']
                net_worth += cc['amount']
        # 操作次数
        oper_count = 0

        # 计算当前的仓位，计算当前现金
        positions = {}
        # 现金或者负债
        cash = net_worth

        # 负债和利息
        debt, interest = PositionMgr.get_debt(acount['name'], cut_off_day)

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
            # # count == 0 and  stock_code 为空，说明是借款、还款，price 不为0
            # if rc['count'] == 0 and rc['stock_code'] == '':
            #     cash += rc['price']
            #     # if rc['price'] < 0:
            #     #     interest += (-rc['price'])
            #     continue

            #  count == 0 代表直接分红，stock_code不为空
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
                buy_cost = PositionMgr.buy_cost(acount['trading_commission'], amount, rc['stock_code'])
                trading_cost += buy_cost
                cash -= (amount + buy_cost)
                continue
            if rc['type'] == 'sell':
                oper_count += 1
                amount = rc['price'] * rc['count']
                sell_cost = PositionMgr.sell_cost(acount['trading_commission'], amount, rc['stock_code'])
                cash += (amount - sell_cost)
                trading_cost += sell_cost
                continue

        # 市值
        market_value = 0
        # 计算当前仓位的股票市值,从而确定当前净值
        if positions:
            if realtime:
                pos_price = PositionMgr.get_realtime_price(positions)
            else:
                pos_price = DataHandler.get_history_price(positions, cut_off_day)

            for code in positions:
                market_value += positions[code] * pos_price[code]

        # 现金要加上借款
        cash += (debt - interest)
        cash = round(cash, 2)

        # 当前净值
        net_worth = round(market_value + cash - debt, 2)

        # 仓位比
        position_rate = round(100 * market_value / net_worth, 3)
        # 收益率
        return_rate = round(100 * (net_worth - initial_capital) / initial_capital, 2)

        trading_cost = round(trading_cost, 3)
        market_value = round(market_value, 3)
        return net_worth, cash, debt, market_value, return_rate, position_rate, trading_cost, interest, oper_count

    @staticmethod
    def stat_position(name,drop = False):

        account = PositionMgr.get_account(name)
        records = PositionMgr.get_trading_record(name)
        days = u_read_input(DataHandler.CAL_TXT)

        conn = sqlite3.connect(PositionMgr.POSMGR_DB)
        cursor = conn.cursor()
        table_name = PositionMgr.TRDSTAT_TB
        if drop:
            cursor.execute("drop table if exists '%s'" % table_name)
        cursor.execute(
            '''create table if not exists '%s' (
                                    name TEXT,
                                    tdate TEXT,
                                    ntcp real,
                                    cash real,
                                    debt real,
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

        insert_sql = "insert into '%s' values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" % table_name

        days = [day for day in days if day >= account['date_created'] and day not in done_days]
        for day in days:
            (net_worth, cash, debt, market_value,
             return_rate, position_rate, trading_cost, interest, oper_count) = PositionMgr.stat_trading_records(
                account,
                records, day)

            (profit_count, profit_amount,
             loss_count, loss_amount) = PositionMgr.analyze_trading_records(
                account['trading_commission'], records, day)
            cursor.execute(insert_sql, (name, day, net_worth, cash, debt,
                                        market_value, return_rate, position_rate,
                                        trading_cost, interest, oper_count,
                                        profit_count, profit_amount,
                                        loss_count, loss_amount
                                        ))
        conn.commit()

    @staticmethod
    def report_position(name, begin_date, end_date, report_file_ext):
        ac = PositionMgr.get_account(name)
        date_created = ac['date_created']
        if begin_date< date_created:
            begin_date = date_created

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

            net_worth = ac['initial_net_worth']

            for cc in ac['capital_changed']:
                if cc['date_changed'] <= date_created:
                    net_worth += cc['amount']
            base_rec = (name, date_created, net_worth, net_worth, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

        end_rec = report_rec[-1]

        # 净值变化
        dif_ntcp = round(end_rec[2] - base_rec[2], 2)

        # 收益率
        rr = round((end_rec[2] / base_rec[2] - 1) * 100, 2)

        # 指数基准收益率
        base_inds = DataHandler.get_index_base(base_rec[1], report_rec[-1][1])

        # 平均市值
        avg_mv = round(sum([e[5] for e in report_rec]) / len(report_rec), 2)

        # 平均仓位
        avg_pr = round(sum([e[7] for e in report_rec]) / len(report_rec), 2)

        # 交易成本
        dif_tcost = round(end_rec[8] - base_rec[8], 2)

        # 融资利息
        dif_itrst = round(end_rec[9] - base_rec[9], 2)

        # 操作次数
        dif_opc = end_rec[10] - base_rec[10]

        # 盈利次数
        dif_prfc = end_rec[11] - base_rec[11]

        # 盈利总额
        dif_prfa = round(end_rec[12] - base_rec[12], 2)

        # 亏损次数
        dif_lsc = end_rec[13] - base_rec[13]

        # 亏损总额
        dif_lsa = round(end_rec[14] - base_rec[14], 2)

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
        with open('data/reports/%s_%s.txt' % (name, report_file_ext), 'w',encoding='utf-8') as wfile:
            wfile.write(info)
        pass

    @staticmethod
    def get_stock_cost(name, stock_code):
        account = PositionMgr.get_account(name)
        trading_commission = account['trading_commission']
        conn = sqlite3.connect(PositionMgr.POSMGR_DB)
        cursor = conn.cursor()
        table_name = PositionMgr.TRDREC_TB
        cursor.execute(
            "select * from '%s' where name = '%s' and stock_code = '%s' order by trading_date" % (
                table_name, name, stock_code))
        records = [{'name': row[0],
                    'stock_code': row[1],
                    'type': row[2],
                    'trading_date': row[3],
                    'count': row[4],
                    'price': row[5]} for row in cursor]

        cost = 0
        total_count = 0
        pre_date = ''
        last_cost = 0
        last_price = 0
        for rc in records:

            # 不是同一天接上的，卖空之后再买入，之前成本清零
            if total_count == 0 and pre_date and pre_date != rc['trading_date']:
                cost = 0
            pre_date = rc['trading_date']

            #  count == 0 代表直接分红
            if rc['count'] == 0:
                cost -= rc['price']
                continue
            #  price == 0 代表直接送转股票，不影响利润
            if rc['price'] == 0:
                total_count += rc['count']
                continue
            # 买入，消耗利润
            if rc['type'] == 'buy':
                # 总金额
                total_count += rc['count']
                amount = rc['price'] * rc['count']
                buy_cost = PositionMgr.buy_cost(trading_commission, amount, rc['stock_code'])
                cost += (amount + buy_cost)
                last_price = rc['price']
                continue
            # 卖出 获得利润
            if rc['type'] == 'sell':
                total_count -= rc['count']
                amount = rc['price'] * rc['count']
                sell_cost = PositionMgr.sell_cost(trading_commission, amount, rc['stock_code'])
                cost -= (amount - sell_cost)
                last_price = rc['price']
        cost += PositionMgr.sell_cost(trading_commission, last_price * total_count, stock_code)
        return round(cost, 3)

    @staticmethod
    def report_position_realtime(name):

        conn = sqlite3.connect(PositionMgr.POSMGR_DB)
        cursor = conn.cursor()
        table_name = PositionMgr.TRDSTAT_TB
        cursor.execute("select * from '%s' where name='%s' order by tdate desc limit 2 " % (
            table_name, name))
        rec = [row for row in cursor]
        # report_rec = rec[0:1]
        # base_rec = rec[1:2]
        # if base_rec:
        #     base_rec = base_rec[0]
        # else:
        #     # 根据账户创建信息，构建基准记录
        #     ac = PositionMgr.get_account(name)
        #     net_worth = ac['initial_net_worth']
        #     date_created = ac['date_created']
        #
        #     for cc in ac['capital_changed']:
        #         if cc['date_changed'] <= date_created:
        #             net_worth += cc['amount']
        #     base_rec = (name, date_created, net_worth, net_worth, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        # end_rec = report_rec[-1]

        end_rec = rec[0]
        account = PositionMgr.get_account(name)
        # 计算总资本，
        initial_capital = account['initial_capital']
        for cc in account['capital_changed']:
            initial_capital += cc['amount']

        records = PositionMgr.get_trading_record(name)

        (net_worth, cash, debt, market_value,
         return_rate, position_rate, trading_cost, interest, oper_count) = PositionMgr.stat_trading_records(account,
                                                                                                            records,
                                                                                                            u_day_befor(
                                                                                                                0),
                                                                                                            True)
        # 昨日净值
        ntcp = end_rec[2]

        # 总收益
        total_rr = round((net_worth / initial_capital - 1) * 100, 2)

        # 净值变化
        dif_ntcp = round(net_worth - ntcp, 2)
        # 收益率
        rr = round((net_worth / ntcp - 1) * 100, 2)

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
            pos_price = PositionMgr.get_realtime_price(positions)
            code2name = Fundamental.get_codes()
            for code in positions:
                if positions[code] != 0:
                    mv = round(positions[code] * pos_price[code], 2)
                    cost = PositionMgr.get_stock_cost(name, code)
                    rate = round((mv / cost - 1) * 100, 3)
                    prf_desc = '%12s元 (%6s%%)' % (round(mv - cost, 2), rate)

                    pr = round(mv * 100 / net_worth, 2)

                    pos_infos.append('%-6s    市值:  %12s元     盈亏:   %16s    仓位占比  %s%%' % (
                        code2name[code], mv, prf_desc, pr))

        info = '''账户:   %s 
账户总收益:     %s %%       
净值变化:       %8s 元           
账户收益率:       %s%%

当前净值:        %8s  元
当前市值：       %8s  元           
可用现金:        %8s  元
当前负债:        %8s  元
利息总计:        %8s  元

当前仓位:        %8s %%             
%s''' % (
            name, total_rr, dif_ntcp, rr,

            net_worth, market_value, cash, debt, interest, position_rate,
            '\n'.join(pos_infos),
        )
        with open('data/reports/%s_realtime.txt' % (name,), 'w',encoding='utf-8') as wfile:
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
            base_rec = (name, date_created, net_worth, net_worth, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        end_rec = report_rec[-1]

        # 总收益
        total_rr = end_rec[6]
        # 当前净值
        ntcp = end_rec[2]
        # 净值变化
        dif_ntcp = round(end_rec[2] - base_rec[2], 2)
        # 收益率
        rr = round((end_rec[2] / base_rec[2] - 1) * 100, 2)
        # 当前总市值
        tmv = end_rec[5]
        # 当前仓位
        tpr = end_rec[7]
        # 当前可用现金
        cash = end_rec[3]
        # 当前负债
        debt = end_rec[4]
        # 利息总计
        interest = end_rec[9]
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
                    mv = round(positions[code] * pos_price[code], 2)
                    cost = PositionMgr.get_stock_cost(name, code)
                    rate = round((mv / cost - 1) * 100, 3)
                    prf_desc = '%12s元 (%6s%%)' % (round(mv - cost, 2), rate)

                    pr = round(mv * 100 / ntcp, 2)

                    pos_infos.append('%-6s    市值:  %12s元     盈亏:   %16s    仓位占比  %s%%' % (
                        code2name[code], mv, prf_desc, pr))

        info = '''账户:   %s 
账户总收益:     %s %%       
净值变化:       %8s 元           
账户收益率:       %s%%
%s

当前净值:        %8s  元
当前市值：       %8s  元           
可用现金:        %8s  元
当前负债:        %8s  元
利息总计:        %8s  元

当前仓位:        %8s %%             
%s''' % (
            name, total_rr, dif_ntcp, rr,
            '\n'.join(base_inds_info),
            ntcp, tmv, cash, debt, interest, tpr,
            '\n'.join(pos_infos),
        )
        with open('data/reports/%s_today.txt' % (name,), 'w',encoding='utf-8') as wfile:
            wfile.write(info)
        pass

    @staticmethod
    def trade(name, type, stock_name, count, price, date_='', is_finance=False):
        names = Fundamental.get_names()
        stock_code = ''
        if stock_name in names:
            stock_code = names[stock_name]
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
                                    price real,
                                    stock_name TEXT)'''
            % table_name)
        insert_sql = "insert into '%s' values (?,?,?,?,?,?,?)" % table_name
        if not date_:
            date_ = u_day_befor(0)
        cursor.execute(insert_sql,
                       (name, stock_code, type, date_, count, price, stock_name))
        conn.commit()
        if is_finance and type == 'buy':
            amount = price * count
            account = PositionMgr.get_account(name)
            buy_cost = PositionMgr.buy_cost(account['trading_commission'], amount, stock_code)
            PositionMgr.fiance(name, 'borrow', stock_name, amount + buy_cost, date_)
            pass
        if is_finance and type == 'sell':
            amount = price * count
            account = PositionMgr.get_account(name)
            sell_cost = PositionMgr.sell_cost(account['trading_commission'], amount, stock_code)
            debt, interest = PositionMgr.get_debt(name, date_, stock_name)
            PositionMgr.fiance(name, 'repay', stock_name, min(amount - sell_cost, debt), date_)
            pass

    # 借钱 还钱
    @staticmethod
    def fiance(name, type, stock, amount, date_=''):
        PositionMgr.backup()
        conn = sqlite3.connect(PositionMgr.POSMGR_DB)
        cursor = conn.cursor()
        table_name = PositionMgr.FIANCE_TB
        cursor.execute(
            '''create table if not exists '%s' (
                                    name TEXT,
                                    type TEXT,
                                    stock TEXT,
                                    trading_date TEXT,
                                    amount real)'''
            % table_name)
        insert_sql = "insert into '%s' values (?,?,?,?,?)" % table_name
        if not date_:
            date_ = u_day_befor(0)
        cursor.execute(insert_sql,
                       (name, type, stock, date_, amount))
        conn.commit()
        pass

    @staticmethod
    def repay_all(name, date_):
        debt, interest = PositionMgr.get_debt(name, date_)
        PositionMgr.fiance(name, 'repay', debt, '', date_)
        pass

    # 获取债务
    @staticmethod
    def get_debt(name, cut_off_day, stock=''):
        account = PositionMgr.get_account(name)
        conn = sqlite3.connect(PositionMgr.POSMGR_DB)
        cursor = conn.cursor()
        table_name = PositionMgr.FIANCE_TB
        cursor.execute(
            "select * from '%s' where name = '%s' order by trading_date" % (table_name, name))
        records = [{'name': row[0],
                    'type': row[1],
                    'stock': row[2],
                    'trading_date': row[3],
                    'amount': row[4]} for row in cursor]
        all_borrow = 0
        all_repay = 0
        all_debt = 0
        pre_day = ''
        for r in records:
            if r['trading_date'] > cut_off_day:
                break

            if stock and stock != r['stock']:
                continue

            days = u_days_diff(pre_day, r['trading_date'])
            # all_debt = all_debt * math.pow(1 + account['interest_rate'], days)
            all_debt = all_debt * (1 + account['interest_rate'] * days)
            if r['type'] == 'borrow':
                all_debt += r['amount']
                all_borrow += r['amount']
            else:
                all_debt -= r['amount']
                all_repay += r['amount']
            pre_day = r['trading_date']
        all_debt = all_debt * (1 + account['interest_rate'] * u_days_diff(pre_day, cut_off_day))
        # all_debt = all_debt * math.pow(1 + account['interest_rate'], u_days_diff(pre_day, cut_off_day))
        all_debt = round(all_debt, 3)
        return all_debt, round(all_debt - all_borrow + all_repay, 3)

    @staticmethod
    def daily_run():
        PositionMgr.stat_position('刘波',True)
        PositionMgr.stat_position('宋茂东')
        PositionMgr.stat_position('宋1')

        PositionMgr.report_position_last_day('刘波')
        PositionMgr.report_position_last_day('宋茂东')
        PositionMgr.report_position_last_day('宋1')

        PositionMgr.export_position()

        PositionMgr.report_position('刘波', u_day_befor(0), u_day_befor(0), 'd')
        PositionMgr.report_position('宋茂东', u_day_befor(0), u_day_befor(0), 'd')
        PositionMgr.report_position('宋1', u_day_befor(0), u_day_befor(0), 'd')

        PositionMgr.report_position('刘波', u_week_begin(), u_day_befor(0), 'w')
        PositionMgr.report_position('宋茂东', u_week_begin(), u_day_befor(0), 'w')
        PositionMgr.report_position('宋1', u_week_begin(), u_day_befor(0), 'w')

        PositionMgr.report_position('刘波', u_month_begin(), u_day_befor(0), 'm')
        PositionMgr.report_position('宋茂东', u_month_begin(), u_day_befor(0), 'm')
        PositionMgr.report_position('宋1', u_month_begin(), u_day_befor(0), 'm')

        PositionMgr.report_position('刘波', u_year_begin(), u_day_befor(0), 'y')
        PositionMgr.report_position('宋茂东', u_year_begin(), u_day_befor(0), 'y')
        PositionMgr.report_position('宋1', u_year_begin(), u_day_befor(0), 'y')
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
    def calc_initial_cash(name, cash):
        account = PositionMgr.get_account(name)
        records = PositionMgr.get_trading_record(name)

        initial_cash = 0
        for cc in account['capital_changed']:
            initial_cash += cc['amount']

        # 计算当前的仓位，计算当前现金
        positions = {}

        # 负债和利息
        debt, interest = PositionMgr.get_debt(account['name'], u_day_befor(0))

        for rc in records:
            # 计算仓位
            if rc['stock_code']:
                if rc['stock_code'] in positions:
                    positions[rc['stock_code']] += (rc['count'] if rc['type'] == 'buy' else -rc['count'])
                else:
                    positions[rc['stock_code']] = (rc['count'] if rc['type'] == 'buy' else -rc['count'])

            #  count == 0 代表直接分红，stock_code不为空
            if rc['count'] == 0:
                initial_cash += rc['price']
                continue

            #  price == 0 代表直接送转股票，不影响现金
            if rc['price'] == 0:
                continue
            # 买入，消耗现金
            if rc['type'] == 'buy':
                # 总金额
                amount = rc['price'] * rc['count']
                buy_cost = PositionMgr.buy_cost(account['trading_commission'], amount, rc['stock_code'])
                initial_cash -= (amount + buy_cost)
                continue
            if rc['type'] == 'sell':
                amount = rc['price'] * rc['count']
                sell_cost = PositionMgr.sell_cost(account['trading_commission'], amount, rc['stock_code'])
                initial_cash += (amount - sell_cost)
                continue

        # 现金要加上借款
        initial_cash += (debt - interest)
        return round(cash - initial_cash, 2)

    @staticmethod
    def do_trading():
        PositionMgr.backup()

        # # 0909
        # PositionMgr.trade('宋茂东', 'buy', '中贝通信', 2900, 30.80, '20190930')
        # PositionMgr.trade('宋茂东', 'buy', '中贝通信', 3000, 31.00, '20190930')

        # # 0911
        # PositionMgr.trade('宋茂东', 'buy', '中贝通信', 5500, 30.18, '20190930')

        # #0916
        # PositionMgr.trade('宋茂东', 'sell', '中贝通信', 5400, 30.701, '20190930')

        # #0917
        # PositionMgr.trade('宋茂东', 'buy', '中贝通信', 600, 30.15, '20190930')
        # PositionMgr.trade('宋茂东', 'buy', '中贝通信', 3000, 30.12, '20190930')
        # PositionMgr.trade('宋茂东', 'buy', '中贝通信', 2000, 29.83, '20190930')

        # #0920
        # PositionMgr.trade('宋茂东', 'sell', '中贝通信', 5600, 30.45, '20190930')

        # #0923
        # PositionMgr.trade('宋茂东', 'buy', '中贝通信', 1500, 30.02, '20190930')

        # #0924
        # PositionMgr.trade('宋茂东', 'sell', '中贝通信', 3500, 30.88, '20190930')

        # #0925
        # PositionMgr.trade('宋茂东', 'buy', '中贝通信', 3000, 30.07, '20190930')
        #
        # PositionMgr.trade('刘波', 'buy', '创维数字', 24500, 8.58, '20190930')
        # PositionMgr.trade('刘波', 'buy', '光韵达', 11700, 11.380, '20190930')
        # PositionMgr.trade('刘波', 'buy', '涪陵榨菜', 7100, 22.33, '20190930')
        # PositionMgr.trade('刘波', 'buy', '星云股份', 6400, 16.68, '20190930')
        # PositionMgr.trade('刘波', '', '', 0, 133.9, '20190930')
        #
        # PositionMgr.trade('宋茂东', 'buy', '创维数字', 34500, 8.58, '20190930')
        #
        # PositionMgr.trade('宋茂东', 'buy', '天奥电子', 4500, 30.60, '20190930')
        # PositionMgr.trade('宋茂东', '', '', 0, 136.22, '20190930')
        # PositionMgr.trade('宋茂东', 'buy', '泰晶科技', 1500, 21.33, '20191010')
        # PositionMgr.trade('宋茂东', 'buy', '中兴通讯', 1100, 33.07, '20191010')
        #
        # PositionMgr.trade('宋茂东', 'sell', '创维数字', 34500, 10.15, '20191011')
        # PositionMgr.trade('刘波', 'sell', '创维数字', 10000, 10.16, '20191011')
        # PositionMgr.trade('刘波', 'sell', '创维数字', 14500, 10.23, '20191011')
        # PositionMgr.trade('刘波', 'sell', '涪陵榨菜', 7100, 22.39, '20191011')
        # PositionMgr.trade('刘波', 'sell', '星云股份', 6400, 16.78, '20191011')
        # PositionMgr.trade('刘波', 'buy', '光韵达', 5000, 11.43, '20191011')
        #
        # PositionMgr.trade('宋茂东', 'buy', '泰晶科技', 1500, 21.50, '20191014')
        # PositionMgr.trade('宋茂东', 'buy', '中兴通讯', 1100, 33.56, '20191014')
        # PositionMgr.trade('宋茂东', 'buy', '中贝通信', 3000, 27.60, '20191014')
        # PositionMgr.trade('宋茂东', 'buy', '中贝通信', 7100, 27.70, '20191014')
        #
        # PositionMgr.trade('刘波', 'buy', '盈趣科技', 1000, 44.28, '20191014')
        # PositionMgr.trade('刘波', 'buy', '盈趣科技', 1000, 43.57, '20191014')
        # PositionMgr.trade('刘波', 'buy', '中新赛克', 500, 105, '20191014')
        # PositionMgr.trade('刘波', 'buy', '中新赛克', 500, 107.66, '20191014')
        # PositionMgr.trade('刘波', 'buy', '中新赛克', 500, 106.15, '20191014')
        # PositionMgr.trade('刘波', 'buy', '光韵达', 5000, 11.74, '20191014')
        # PositionMgr.trade('刘波', 'buy', '光韵达', 9200, 11.72, '20191014')
        # PositionMgr.trade('刘波', 'buy', '纵横通信', 2000, 22.77, '20191014')
        # PositionMgr.trade('刘波', 'buy', '盈趣科技', 1900, 44.22, '20191015')
        # PositionMgr.trade('刘波', 'buy', '中新赛克', 1500, 107.74, '20191015')
        # PositionMgr.trade('宋茂东', 'buy', '中贝通信', 4000, 26.86, '20191015')
        #
        # PositionMgr.trade('宋茂东', 'buy', '中贝通信', 8000, 26.48, '20191023')
        # PositionMgr.trade('宋茂东', 'buy', '中贝通信', 19000, 26.29, '20191023')
        # PositionMgr.trade('宋茂东', 'buy', '中兴通讯', 7200, 33.89, '20191028')
        #
        # PositionMgr.trade('刘波', 'sell', '光韵达', 30900, 12.70, '20191211')
        # PositionMgr.trade('刘波', 'sell', '中新赛克', 3000, 114.964, '20191212')
        # PositionMgr.trade('刘波', 'sell', '纵横通信', 2000, 23.28, '20191212')
        # PositionMgr.trade('刘波', 'buy', '盈趣科技', 3900, 42.80, '20191212')
        # PositionMgr.trade('刘波', 'sell', '盈趣科技', 3900, 43.28, '20191212')
        #
        # PositionMgr.trade('宋茂东', 'buy', '天奥电子', 5300, 28.12, '20191213')
        # PositionMgr.trade('宋茂东', 'buy', '中兴通讯', 9100, 33.18, '20191213')
        # PositionMgr.trade('宋茂东', 'buy', '泰晶科技', 3000, 16.45, '20191213')
        #
        # PositionMgr.trade('刘波', 'buy', '盈趣科技', 3800, 42.969, '20191213')
        #
        #
        # PositionMgr.trade('刘波', 'buy', '恒瑞医药', 700, 85.06, '20191216')
        # PositionMgr.trade('刘波', 'buy', '成都燃气', 1000, 10.45, '20191217')
        #
        #
        # PositionMgr.trade('宋茂东', 'sell', '泰晶科技', 6000, 21.71, '20191219')
        #
        # PositionMgr.trade('宋茂东', 'buy', '中兴通讯', 3800, 33.70, '20191220')
        #
        #
        #
        # PositionMgr.trade('刘波', 'sell', '成都燃气', 1000, 19.665, '20191223')
        #
        # PositionMgr.trade('刘波', 'sell', '盈趣科技', 7700, 45.57, '20200102')
        #
        # PositionMgr.trade('刘波', 'sell', '恒瑞医药', 1100, 87.82, '20200102')
        #
        # PositionMgr.trade('刘波', 'buy', '贵州茅台', 100, 1122.58, '20200102')
        #
        # PositionMgr.trade('刘波', 'buy', '恒瑞医药', 300, 85.72, '20191223')
        # PositionMgr.trade('刘波', 'buy', '恒瑞医药', 100, 85.69, '20191223')
        #
        # PositionMgr.trade('宋茂东', 'buy', '天奥电子', 19100, 29.743, '20200103')
        #
        # PositionMgr.trade('宋茂东', 'sell', '中兴通讯', 22300, 36.610, '20200103')
        #
        # PositionMgr.trade('宋茂东', 'buy', '纵横通信', 17500, 23.91, '20200106')
        # PositionMgr.trade('刘波', 'buy', '天奥电子', 5400, 30.05, '20200106')
        # PositionMgr.trade('刘波', 'buy', '纵横通信', 5900, 23.80, '20200106')
        #
        # PositionMgr.trade('宋茂东', 'buy', '纵横通信', 21100, 23.57, '20200107')
        # PositionMgr.trade('宋茂东', 'buy', '纵横通信', 7300, 23.70, '20200107')
        # PositionMgr.trade('刘波', 'sell', '贵州茅台', 100, 1136.12, '20200304')
        #
        # PositionMgr.trade('宋茂东', 'buy', '中贝通信', 5200, 23.00, '20200306')
        # PositionMgr.trade('刘波', 'buy', '中贝通信', 2500, 22.94, '20200306')
        # PositionMgr.trade('刘波', 'buy', '纵横通信', 2200, 25.12, '20200306')
        #
        # PositionMgr.trade('宋茂东', 'sell', '纵横通信', 45900, 27.98, '20200309')
        # PositionMgr.trade('宋茂东', 'buy', '纵横通信', 47900, 27.63, '20200309')
        # PositionMgr.trade('宋茂东', 'sell', '天奥电子', 28900, 35.706, '20200310')
        # PositionMgr.trade('刘波', 'sell', '天奥电子', 5400, 36.03, '20200310')
        # PositionMgr.trade('刘波', 'buy', '中贝通信', 2200, 23.62, '20200310')
        #
        # PositionMgr.trade('宋茂东', 'sell', '纵横通信', 47900, 28.85, '20200310')
        # PositionMgr.trade('宋茂东', 'sell', '中贝通信', 53300, 25.87, '20200310')
        # PositionMgr.trade('宋茂东', 'buy', '卓胜微', 1000, 539.50, '20200310')
        # PositionMgr.trade('宋茂东', 'buy', '纵横通信', 20000, 28.43, '20200310')
        # PositionMgr.trade('宋茂东', 'buy', '中贝通信', 20000, 25.62, '20200310')
        # PositionMgr.trade('宋茂东', 'buy', '纵横通信', 10000, 28.18, '20200311')
        # PositionMgr.trade('宋茂东', 'buy', '纵横通信', 19900, 25.29, '20200331',True)
        # PositionMgr.trade('刘波', 'buy', '纵横通信', 5000, 25.54, '20200331',True)
        # PositionMgr.trade('宋茂东', 'buy', '中贝通信', 20000, 21.36, '20200401',True)
        #
        # PositionMgr.trade('宋茂东', 'buy', '卓胜微', 1300, 510.01, '20200403')
        # PositionMgr.trade('宋茂东', 'buy', '中新赛克', 1700, 159.51, '20200403',True)
        #
        # PositionMgr.trade('宋茂东', 'buy', '精准信息', 6200, 5.77, '20200416')
        #
        # PositionMgr.trade('宋茂东', 'buy', '中贝通信', 4500, 19.83, '20200420')
        #
        # PositionMgr.trade('宋茂东', 'buy','沪硅产业-U', 500, 3.89, '20200420')
        # PositionMgr.trade('宋茂东', 'sell','沪硅产业-U', 500, 10.54, '20200421')
        # PositionMgr.trade('宋茂东', 'sell', '精准信息', 6100, 5.19, '20200429')
        #
        #
        # PositionMgr.trade('宋茂东', 'sell', '卓胜微', 2300, 637.61, '20200430')
        # PositionMgr.trade('宋茂东', 'sell', '中新赛克', 1700, 181.80, '20200430',True)
        #
        # PositionMgr.trade('宋茂东', 'buy', '国电南自', 30000, 10.213, '20200506',True)
        # PositionMgr.trade('宋茂东', 'buy', '中贝通信', 10000, 19.810, '20200506',True)
        # PositionMgr.trade('宋茂东', 'buy', '纵横通信', 11100, 23.490, '20200506',True)
        # PositionMgr.trade('宋茂东', 'buy', '精准信息', 30000, 5.44, '20200506')
        # PositionMgr.trade('宋茂东', 'buy', '中科信息', 14000, 22.34, '20200506')
        # PositionMgr.trade('宋茂东', 'buy', '意华股份', 10000, 32.30, '20200506')
        # PositionMgr.trade('宋茂东', 'buy', '中富通', 10000, 21.85, '20200507')
        # PositionMgr.trade('宋茂东', 'buy', '意华股份', 10000, 33.06, '20200507')

        # PositionMgr.trade('宋1', 'buy', '明阳电路', 600, 19.46, '20200508')
        # PositionMgr.trade('宋1', 'buy', '金杯电工', 2000, 6.0, '20200508')
        # PositionMgr.trade('宋1', 'buy', '航发控制', 1000, 13.37, '20200508')
        # PositionMgr.trade('宋1', 'buy', '游族网络', 600, 19.71, '20200508')
        # PositionMgr.trade('宋1', 'buy', '达安基因', 500, 22.75, '20200508')

        # PositionMgr.trade('宋茂东', 'sell', '国电南自', 30000, 10.32, '20200508',True)

        # PositionMgr.trade('宋1', 'buy', '广和通', 200, 83.59, '20200511')

        # PositionMgr.trade('宋1', 'buy', '元隆雅图', 300, 38.02, '20200512')
        # PositionMgr.trade('宋1', 'buy', '长城证券', 1100, 12.21, '20200512')

        # PositionMgr.trade('宋1', 'sell', '达安基因', 500, 24.58, '20200514')
        # PositionMgr.trade('宋1', 'buy', '游族网络', 600, 20.20, '20200514')

        # PositionMgr.trade('宋茂东', 'sell', '精准信息', 30100, 6.13, '20200514')

        # PositionMgr.trade('宋茂东', 'buy', '中科信息', 8000, 22.95, '20200515')

        # PositionMgr.trade('宋1', '', '金杯电工', 0, 500, '20200518')
        # PositionMgr.trade('宋1', 'buy', '广和通', 0, 80, '20200519')
        # PositionMgr.trade('宋1', 'buy', '广和通', 160, 0, '20200519')

        # PositionMgr.trade('宋茂东', 'buy', '奥特维', 500, 23.280, '20200521')

        # PositionMgr.trade('宋1', '', '明阳电路', 0, 132.00, '20200528')

        PositionMgr.trade('宋茂东', 'buy', '纵横通信', 0, 2440, '20200608')
        PositionMgr.trade('宋茂东', 'buy', '纵横通信', 18300, 0, '20200608')
        PositionMgr.trade('刘波', 'buy', '纵横通信', 0, 524, '20200608')
        PositionMgr.trade('刘波', 'buy', '纵横通信', 3930, 0, '20200608')

        PositionMgr.trade('宋1', 'sell', '游族网络', 1200, 22.10, '20200611')
        PositionMgr.trade('宋1', 'sell', '广和通', 360, 55.13, '20200611')
        PositionMgr.trade('宋1', 'sell', '长城证券', 1100, 12.25, '20200611')
        PositionMgr.trade('宋1', 'buy', '游族网络', 500, 21.80, '20200612')
        PositionMgr.trade('宋1', 'buy', '元隆雅图', 300, 36.40, '20200612')
        PositionMgr.trade('宋1', 'sell', '元隆雅图', 300, 38.61, '20200612')



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
        sp11, sp12 = export_position_fu('宋1')
        u_write_to_file('data/position.txt', lp1 | sp1 | sp11)
        u_write_to_file_append('data/attention.txt', lp2 | sp2 | sp12)


if __name__ == '__main__':
    import cProfile

    # import tushare as ts
    # df = ts.get_hist_data('600848', start='2019-10-05', end='2019-11-09',ktype='5')
    # print(df)
    PositionMgr.do_trading()
    # PositionMgr.daily_run()
    # # PositionMgr.export_position()
    PositionMgr.report_position_realtime('宋茂东')
    PositionMgr.report_position_realtime('宋1')
    PositionMgr.report_position_realtime('刘波')
    # print(PositionMgr.calc_initial_cash('宋茂东', 332.06))
    # print(PositionMgr.calc_initial_cash('刘波', 9.85))
    # print(PositionMgr.get_debt('宋茂东', u_day_befor(0)))
    # print(PositionMgr.get_debt('刘波', u_day_befor(0)))
    # print(PositionMgr.calc_initial_cash('刘波', 1009.85))

    # print(PositionMgr.get_debt('宋茂东', u_day_befor(0), '纵横通信'))
    # print(PositionMgr.get_debt('宋茂东', u_day_befor(0), '中贝通信'))
    # print(PositionMgr.get_debt('宋茂东', u_day_befor(0), '中新赛克'))
    # print(PositionMgr.get_debt('刘波', u_day_befor(0)))

    # PositionMgr.update_trading_record()
    # PositionMgr.daily_run()
    # PositionMgr.report_position('刘波', '20180101', '20191001')
    # PositionMgr.report_position('宋茂东', '20180101', '20191001')
