from dataprocess import dh_daily_run, dh_check_macd
from picking import check_acti, picking_daily
from macd_bts import find_macd_target
from synthesis import daily_synthesis

from fundamental import Fundamental

from position import PositionMgr


def daily_run():
    # DataHandler.run_daily_batch()
    # DataHandler.download_concept_detail()
    # DataHandler.cook_raw_db()
    # DataHandler.create_today_table()
    # DataHandler.download_index()
    # DataHandler.create_attention_ex()
    #
    dh_daily_run()
    picking_daily()
    find_macd_target()
    Fundamental.daily_run()
    PositionMgr.daily_run()
    daily_synthesis()
    pass


if __name__ == '__main__':
    import cProfile

    cProfile.run('daily_run()')
    # cProfile.run('real_time_monitor()')

    pass
