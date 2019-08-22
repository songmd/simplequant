from backtest import *


# 涨停策略
class LimitupStrategy(object):
    def __init__(self):
        self.need_record = True
        # 到了多少个涨停才买
        self.limitup_threshold = 1
        self.query_column = ['open_', 'close_', 'trade_date']
        self.reset('')

    def result_table(self):
        return 'limitup_th%s' % self.limitup_threshold

    def get_id(self):
        return 'limitup_th%s' % self.limitup_threshold

    def back_test(self, code, row):
        ret = None
        # 第一天，不操作,不认为有涨停
        if self._pre_close <= 0:
            self._pre_close = row[1]
            return ret
        # 涨停价位
        limit_price = round(self._pre_close * 1.1, 2) - 0.1

        # 已有仓位,说明是昨天买的
        if self._has_position:
            # 以今天收盘价卖出
            self._has_position = False
            ret = (code, self._b_price, row[1], self._b_date, row[2])
        else:
            # 之前的涨停数达到要求
            # 且开盘没涨停,买
            if self._limitup_count >= self.limitup_threshold and row[0] < limit_price:
                self._has_position = True
                self._b_price = row[0]
                self._b_date = row[2]

        # 只要涨停了，计数累加
        if row[1] >= limit_price:
            self._limitup_count += 1
        else:
            # 没涨停计数清零
            self._limitup_count = 0

        self._pre_close = row[1]
        return ret

    def reset(self, code):
        self._b_date = ''
        self._b_price = 0
        self._pre_close = 0
        # 之前涨停数目
        self._limitup_count = 0
        # 是否有持仓
        self._has_position = False


def profile_run():
    begin_date = u_month_befor(6)
    end_date = u_month_befor(0)

    strategy = LimitupStrategy()
    strategy.limitup_threshold = 1
    run_backtest(strategy, '', begin_date, end_date)
    analyze_backtest_by_code(strategy.result_table(), '', begin_date, end_date)



if __name__ == '__main__':
    import cProfile

    # cProfile.run('profile_run2()')
    profile_run()
    pass
