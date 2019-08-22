import sqlite3


# 账户 交易员
class AccountEx(object):
    def __init__(self):
        # 交易员唯一id
        self.id = -1
        # 起始金额
        self.start_amount = 0
        # 可用资金
        self.amount = 0

        self.trans_record = []
        # 股票持仓
        self.positions = {}

        # 交易员姓名
        self.name = ''

        # 资金策略id
        self.strategy_id = -1

        # 资金策略参数
        self.strategy_para = {}

    def dict_2_str(self, dct):
        return "\n".join(['%s:%s' % (k, dct[k]) for k in dct])

    def str_2_dict(self, ds):
        dct = {}
        for pos in ds.split():
            p = pos.split(":")
            if len(p) == 2:
                dct[p[0]] = int(p[1])
        return dct

    def save_records(self):
        conn = sqlite3.connect('data/trans.db')
        cursor = conn.cursor()
        table_sql = '''create table if not exists records(
                            t_id INTEGER,
                            s_id INTEGER,
                            time TEXT,
                            code TEXT,
                            price REAL ,
                            type TEXT,
                            win INTEGER,
                            lot INTEGER,
                            reason TEXT)'''
        cursor.execute(table_sql)

        for record in self.trans_record:
            insert_sql = "INSERT INTO records (t_id,s_id,time,code,price,type,win,lot,reason) VALUES (?,?,?,?,?,?,?,?,?)"
            cursor.execute(insert_sql,
                           (self.id, self.strategy_id, record['time'],
                            record['code'], record['price'], record['type'], record['win'], record['lot'],
                            record['reason']))
        conn.commit()

    def save(self):
        conn = sqlite3.connect('trans.db')
        cursor = conn.cursor()
        table_sql = '''create table if not exists accounts(
                        id  INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                        name TEXT,
                        start_amount REAL ,
                        amount REAL ,
                        positions TEXT,
                        strategy_id INTEGER,
                        strategy_para TEXT)'''
        cursor.execute(table_sql)

        if self.id <= 0:
            # 添加数据库
            insert_sql = "INSERT INTO accounts (name,start_amount,amount,positions,strategy_id,strategy_para) VALUES (?,?,?,?,?,?)"
            rc = cursor.execute(insert_sql, (
                self.name, self.start_amount,
                self.amount, self.dict_2_str(self.positions),
                self.strategy_id,
                self.dict_2_str(self.strategy_para)))
            self.id = cursor.lastrowid
        else:
            # 修改数据库
            update_sql = "update accounts set name=?,start_amount=?,amount=?,positions=?,strategy_id=?,strategy_para=? where id=?"
            cursor.execute(update_sql, (
                self.name, self.start_amount, self.amount, self.dict_2_str(self.positions), self.strategy_id,
                self.dict_2_str(self.strategy_para),
                self.id))
        conn.commit()

    # 获得股票买入价格
    def get_code_price(self, code):
        for record in reversed(self.trans_record):
            if record['code'] == code:
                return record['price']
        return None

    # 得到止损值
    def stop_loss(self):
        sl = -99.00
        if 'stop_loss' in self.strategy_para:
            sl = -abs(float(self.strategy_para['stop_loss']))
            sl = sl if sl < -2.0 else -2.0
        return sl / 100.00

    def has_position(self, code):
        return code in self.positions and self.positions[code] > 0

    def get_codes(self):
        return {c for c in self.positions if self.positions[c] > 0}

    def macd_strategy_fix_code(self, code, time, close, trend):
        if self.has_position(code):
            if trend <= -1:
                return self.sell(code, time, close, 'trend=%s' % trend)
            elif close / self.get_code_price(code) - 1 < self.stop_loss():
                return self.sell(code, time, close, '止损点 %s' % self.stop_loss())

        elif trend >= 2:
            return self.buy(code, time, close, 'trend=%s' % trend)
        return 0

    # def macd_strategy_select_code(self, cd, time):
    #     # 先处理该卖的
    #
    #     for code in self.positions.keys():
    #         if self.has_position(code) and code in cd:
    #             if cd[code][1] <= -2:
    #                 self.sell(code, time, cd[code][0], 'trend=%s' % cd[code][1])
    #             else:
    #                 buy_price = self.get_code_price(code)
    #                 if cd[code][0] / buy_price - 1 < self.stop_loss():
    #                     self.sell(code, time, cd[code][0], '止损点 %s' % self.stop_loss())
    #     # 选出有买点的票
    #     bts = [[c, cd[c][0], cd[c][1]] for c in cd if cd[c][1] >= 2]
    #     if bts:
    #         buy_taget = random.choice(bts)
    #         self.buy(buy_taget[0], time, buy_taget[1], 'trend=%s' % buy_taget[2])
    #
    # pass

    def sell(self, code, time, close, reason):
        record = {}
        record['time'] = time
        record['code'] = code
        record['price'] = close
        record['type'] = 's'
        record['win'] = 1 if close > self.get_code_price(code) else -1
        record['lot'] = self.positions[code]
        record['reason'] = reason

        self.amount += self.positions[code] * close * 100
        self.positions.pop(code)
        self.trans_record.append(record)
        return -record['lot']

    def buy(self, code, time, close, reason):
        lot = int(self.amount / (100 * close))
        if lot >= 1:
            self.amount -= 100 * close * lot
            self.positions[code] = lot
            record = {}
            record['time'] = time
            record['code'] = code
            record['price'] = close
            record['type'] = 'b'
            record['win'] = 0
            record['lot'] = self.positions[code]
            record['reason'] = reason
            self.trans_record.append(record)
            return lot
        return 0

    def evaluate(self):
        if (len(self.trans_record) <= 0):
            return None

        win_time = 0
        buy_time = 0

        # 持仓股票,对应的市值，基于最后买入价，用于计算市值
        code_mv = {c: 0 for c in self.positions if self.positions[c] > 0}

        for record in self.trans_record:
            if record['win'] == 1:
                win_time += 1
            if record['type'] == 'b':
                if record['code'] in code_mv:
                    code_mv[record['code']] = record['price'] * record['lot'] * 100
                buy_time += 1

        markt_val = 0
        for code in code_mv:
            markt_val += code_mv[code]

        rt_rate = round((self.amount + markt_val - self.start_amount) / self.start_amount, 3)

        w_rate = round(win_time * 100.0 / buy_time, 3)

        return {'w_rate': w_rate, 'win_time': win_time, 'buy_time': buy_time, 'rt_rate': rt_rate}
