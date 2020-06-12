from backtest import *
from dataprocess import DataHandler

# 涨停策略
class MacdStrategy(object):
    def __init__(self):
        self.need_record = True

        self.query_column = ['close_', 'trend', 'trade_date']
        self.buy_trend = 2
        self.sell_trend = -1
        self.stop_loss = 10.0
        self.reset('')

    def result_table(self):
        return 'macd_bt%s_st%s' % (self.buy_trend, self.sell_trend)

    def get_id(self):
        return 'macd_bt%s_st%s' % (self.buy_trend, self.sell_trend)

    def need_stop_loss(self, now_price):
        if self.stop_loss == 0:
            return False
        loss = (1 - now_price / self._b_price) * 100
        return loss > self.stop_loss

    def back_test(self, code, row):
        ret = None

        # 已有仓位,说明是昨天买的
        if self._has_position:
            if row[1] <= self.sell_trend or self.need_stop_loss(row[0]):
                # 以今天收盘价卖出
                self._has_position = False
                ret = (code, self._b_price, row[0], self._b_date, row[2])
        else:
            if row[1] >= self.buy_trend:
                self._has_position = True
                self._b_price = row[0]
                self._b_date = row[2]

        return ret

    def reset(self, code):
        self._b_date = ''
        self._b_price = 0

        # 是否有持仓
        self._has_position = False


def find_macd_target():
    begin_date = '20170101'
    end_date = u_month_befor(0)

    strategy = MacdStrategy()
    strategy.stop_loss = 10.0
    # run_backtest(strategy, '603106', begin_date, end_date)
    # run_backtest(strategy, 'macd_target.txt', begin_date, end_date)
    run_backtest(strategy, '', begin_date, end_date)
    analyze_backtest_by_code(strategy.result_table(), u_month_befor(12), end_date, u_create_path_by_system('t_macd_all.txt'))
    # win_time >= 8 and win_rate > 50 and rt_rate > 0

    # analyze_backtest_by_time(strategy.result_table(), begin_date, end_date, 7)
    # analyze_backtest_by_time(strategy.result_table(), begin_date, end_date, 30)
    analyze_backtest_by_code_time(strategy.result_table(), begin_date, end_date, 120, u_create_path_by_system('t_macd_jx.txt'))
    # analyze_backtest_by_time(strategy.result_table(), begin_date, end_date, 90)
    # analyze_backtest_by_time(strategy.result_table(), begin_date, end_date, 250)

    DataHandler.select_today_macd()




if __name__ == '__main__':
    import cProfile

    # cProfile.run('find_macd_target()')
    find_macd_target()
    pass
