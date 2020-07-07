import sqlite3
from utility import *
from sqlalchemy import create_engine
import json
import urllib.request
import os
import lxml
import tushare as ts
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from multiprocessing import Process
import re

# 自动化处理
class Automation(object):
    FUNDA_DB = 'data/fundamental.db'
    WENCAI_COOKIE = 'data/wencai_cookie.txt'
    WENCAI_HOME_URL = 'http://www.iwencai.com/?allow_redirect=false'
    DOWNLOAD_PATH = u_get_download_path()
    ZENJIANCHI = 'data/zenjianchi.xml'
    YUZENJIAN = 'data/yuzenjian.xml'
    JIEJINNEXT = 'data/jiejinnext.xml'
    JIEJINCUR = 'data/jiejincur.xml'
    JIEJINPRE = 'data/jiejinpre.xml'
    YUZJ_TB = 'yuzenjian'
    JIEJIN_TB = 'jiejin'
    ZENJC_TB = 'zenjianchi'
    FINA_TB = 'finance'
    BASIC_TB = 'basic'
    ANAL_FINA_TB = 'analyze_finance'
    INDUSTRY_TB = 'industry'
    OPERATE_TB = 'operate'
    DOCTOR_TB = 'doctor'
    CONCEPT_TB = 'concept'

    @staticmethod
    def rate_text_to_float(rate):
        if rate == '--':
            return 0
        return float(rate[:-1])


if __name__ == '__main__':
    u_write_to_file('data/test.txt',['begin',6,7,9])
    time.sleep(240)
    u_write_to_file('data/endtest.txt',['end',6,7,9])