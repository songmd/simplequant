from dataprocess import DataHandler
from picking import get_picking_info

import os

from fundamental import Fundamental
import sqlite3
from utility import *


def synthesis(input, dir=''):
    file_name = os.path.split(input)[1]
    if dir == '':
        file_path = 'data/dgn/dgn_%s' % (file_name,)
    else:
        path = 'data/dgn/%s' % dir
        u_mk_dir(path)
        file_path = '%s/dgn_%s' % (path, file_name)
    # no = 1
    # while True:
    #     file_path = 'dgn_%s_%s.txt' % (file_name, no,)
    #     if os.path.exists(file_path):
    #         no += 1
    #     else:
    #         break
    with open(file_path, 'w',encoding='utf-8') as wfile:
        codes = u_read_input(input)
        codes = Fundamental.name_to_codes(codes)
        for code in codes:
            wfile.writelines(code + '\n')
        for code in codes:
            fundamental = Fundamental.get_fundamental(code)
            picking_info = get_picking_info(code)
            tech_info = DataHandler.get_tech_index_info(code)
            synthesis_info = '%s\n%s\n%s' % (fundamental, tech_info, picking_info)
            wfile.write(synthesis_info)
            wfile.write("\n\n\n")
    pass


def daily_synthesis():
    synthesis('data/position.txt')
    synthesis('data/attention.txt')
    synthesis('data/selection.txt')
    # synthesis('/Users/hero101/Documents/t_bw_all.txt')
    synthesis(u_create_path_by_system('t_bw_hot.txt'))
    synthesis(u_create_path_by_system('t_doctor_all.txt'))
    synthesis(u_create_path_by_system('t_acti_lht.txt'))
    synthesis(u_create_path_by_system('t_macd_today.txt'))
    synthesis(u_create_path_by_system('t_ss_today.txt'))
    synthesis(u_create_path_by_system('t_acti_check.txt'))
    synthesis(u_create_path_by_system('t_macd_check.txt'))
    synthesis(u_create_path_by_system('t_doctor_check.txt'))
    synthesis(u_create_path_by_system('t_yuzen_check.txt'))


if __name__ == '__main__':
    import cProfile

    # daily_synthesis()
    # synthesis('data/input.txt')
    # synthesis('/Users/hero101/Documents/t_acti_lht.txt')
    # ret = u_file_intersection([
    #     'data/input.txt',
    #     'data/input2.txt',
    #     # '',
    #
    # ])
    # u_write_to_file('data/output.txt',ret)
    # synthesis('data/output.txt')
    # synthesis('data/position.txt')
    # synthesis('data/attention.txt')
    # synthesis('data/selection.txt')
    # synthesis('/Users/hero101/Documents/t_bw_all.txt')
    # synthesis('/Users/hero101/Documents/t_bw_hot.txt')
    # synthesis('/Users/hero101/Documents/t_doctor_all.txt')
    # synthesis('/Users/hero101/Documents/t_acti_today.txt')
    # synthesis('/Users/hero101/Documents/t_macd_today.txt')
    # synthesis('/Users/hero101/Documents/t_ss_today.txt')
    # synthesis('/Users/hero101/Documents/t_acti_check.txt')
    # synthesis('/Users/hero101/Documents/t_macd_check.txt')
    # synthesis('/Users/hero101/Documents/t_doctor_check.txt')
