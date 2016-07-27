# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


from stl_utilities import stl_logger as slog
from stl_utilities import stl_thread_pool as stp
from stl_utilities import stl_file_utilities as sfu
from stl_data_manager.tushare import stl_dm_fundamental as sfund

import os
import datetime
import time
import linecache
import pandas as pd
import tushare


'''
获取证券股票的最新一个交易日行情信息
存入对应的csv文件
'''


def get_all_current_data():
    '''
    获取所有股票的最新一个交易日数据,并将结果保存到对应csv文件

    tushare.get_today_all()查询所有股票最新一个交易日行情, 返回数据如下:
        code：代码
        name:名称
        changepercent:涨跌幅
        trade:现价
        open:开盘价
        high:最高价
        low:最低价
        settlement:昨日收盘价
        volume:成交量
        turnoverratio:换手率

    Parameters
    ------
        code: 股票代码
        type: 数据类型：D=日k线 W=周 M=月 5=5分钟 15=15分钟 30=30分钟 60=60分钟
    return
    -------
        无
    '''
    slog.StlDmLogger().debug('get_all_current_data begin...')

    file_path = '../../data/origin/tushare/security_trade_data/current/current.csv'
    try:
        tmp_data = tushare.get_today_all()
    except Exception as exception:
        slog.StlDmLogger().error('tushare.get_today_all() excpetion, args: %s' % exception.args.__str__())

    if tmp_data is None:
        slog.StlDmLogger().warning('tushare.get_today_all() return none')
    else:
        data_str = tmp_data.to_csv()
        with open(file_path, 'w') as fout:
            fout.write(data_str)
    slog.StlDmLogger().debug('get_all_current_data Finish...')

if __name__ == "__main__":
    get_all_current_data()

