from backtest import *


# 固定策略
class GrowthrateStrategy(object):
    def __init__(self):
        self.need_record = True
        self.growth_rate = 0
        self.sell_strategy = 0
        self.query_column = ['close_', 'open_', 'high', 'low', 'trade_date']
        self.reset('')

    def reset(self, code):
        self._b_date = ''
        self._b_price = 0
        self._pre_close = 0
        # 是否有持仓
        self._has_position = False

    def result_table(self):
        return 'growthrate_gr%s_s%s' % (self.growth_rate, self.sell_strategy)

    def get_id(self):
        return 'growthrate_gr%s_s%s' % (self.growth_rate, self.sell_strategy)

    def back_test(self, code, row):
        ret = None
        # 第一天，不操作,因为无法确定涨幅
        if self._pre_close <= 0:
            self._pre_close = row[0]
            return ret
        # 涨停价位
        limit_price = round(self._pre_close * 1.1, 2) - 0.1

        # 已有仓位,说明是昨天买的
        if self._has_position:
            self._has_position = False
            sell_price = 0
            if self.sell_strategy == 0:
                sell_price = row[0]

            # 开盘价卖出
            elif self.sell_strategy == 1:
                sell_price = row[1]

            # 近似均价卖出
            elif self.sell_strategy == 2:
                sell_price = (row[2] + row[3]) / 2

            ret = (code, self._b_price, sell_price, self._b_date, row[4])
        else:
            limit_price = round(self._pre_close * 1.1, 2) - 0.1
            if round(row[3], 2) < limit_price:  # 不是一字涨停才能买
                target_price = round(self._pre_close * (1 + self.growth_rate / 100), 2)
                if row[2] >= target_price and target_price >= row[3]:
                    self._b_price = target_price
                    self._b_date = row[4]
                    self._has_position = True

        self._pre_close = row[0]
        return ret


def profile_run():
    begin_date = u_month_befor(24)
    end_date = u_month_befor(0)

    strategy = GrowthrateStrategy()

    run_backtest(strategy, '', begin_date, end_date)
    analyze_backtest_by_code(strategy.result_table(), begin_date, end_date)
    analyze_backtest_by_time(strategy.result_table(), begin_date, end_date, 7)
    analyze_backtest_by_time(strategy.result_table(), begin_date, end_date, 30)
    analyze_backtest_by_time(strategy.result_table(), begin_date, end_date, 90)
    analyze_backtest_by_time(strategy.result_table(), begin_date, end_date, 250)


if __name__ == '__main__':
    import cProfile

    # cProfile.run('profile_run2()')
    profile_run()
    pass
