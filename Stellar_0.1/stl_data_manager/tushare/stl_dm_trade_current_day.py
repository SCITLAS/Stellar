# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


from stl_utils import stl_logger as slog

import os
import pandas as pd
import tushare


'''
获取证券股票的最新一个交易日行情信息
存入对应的xlsx文件
'''


DEFAULT_DIR_PATH = '../../../Data/origin/tushare/security_trade_data/trade/current_day'


def get_all_security_current_day_data():
    '''
    获取所有股票的最新一个交易日数据,并将结果保存到对应xlsx文件

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
    slog.StlDmLogger().debug('get_all_security_current_day_data begin...')

    dir_path = DEFAULT_DIR_PATH
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = '%s/trade.xlsx' % dir_path
    tmp_data = pd.DataFrame()
    try:
        tmp_data = tushare.get_today_all()
    except Exception as exception:
        slog.StlDmLogger().error('tushare.get_today_all() excpetion, args: %s' % exception.args.__str__())

    if tmp_data is None:
        slog.StlDmLogger().warning('tushare.get_today_all() return none')
    else:
        tmp_data.to_excel(file_path)

    slog.StlDmLogger().debug('get_all_security_current_day_data Finish...')


if __name__ == "__main__":
    get_all_security_current_day_data()

