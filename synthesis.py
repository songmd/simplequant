from dataprocess import DataHandler
from picking import get_picking_info

import os

from fundamental import Fundamental
import sqlite3
from utility import *


def synthesis(input):
    file_name = os.path.split(input)[1]
    file_path = 'data/dgn_%s' % (file_name,)
    # no = 1
    # while True:
    #     file_path = 'dgn_%s_%s.txt' % (file_name, no,)
    #     if os.path.exists(file_path):
    #         no += 1
    #     else:
    #         break
    with open(file_path, 'w') as wfile:
        codes = u_read_input(input)
        for code in codes:
            wfile.writelines(code + '\n')
        for code in codes:
            fundamental = Fundamental.get_fundamental(code)
            picking_info = get_picking_info(code)
            tech_info = DataHandler.get_tech_index_info(code)
            synthesis_info = '%s\n%s\n%s' % (fundamental, picking_info, tech_info)
            wfile.write(synthesis_info)
            wfile.write("\n\n\n")
    pass

def daily_synthesis():
    synthesis('data/position.txt')
    synthesis('/Users/hero101/Documents/t_acti_today.txt')
    synthesis('/Users/hero101/Documents/t_macd_today.txt')
    synthesis('/Users/hero101/Documents/t_acti_check.txt')
    synthesis('/Users/hero101/Documents/t_macd_check.txt')


if __name__ == '__main__':
    import cProfile

    synthesis('data/input.txt')
    synthesis('data/position.txt')
    synthesis('/Users/hero101/Documents/t_acti_today.txt')
    synthesis('/Users/hero101/Documents/t_macd_today.txt')
    synthesis('/Users/hero101/Documents/t_acti_check.txt')
    synthesis('/Users/hero101/Documents/t_macd_check.txt')
