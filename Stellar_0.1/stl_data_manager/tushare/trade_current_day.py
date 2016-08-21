# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import os

import tushare

from stl_utils.logger import dm_logger


'''
获取证券股票的最新一个交易日行情信息
'''


# Global Consts
USING_CSV = 1
USING_MY_SQL = 2
USING_MONGO_DB = 3
STORAGE_MODE = USING_CSV

# TuShare Data Storage Path
DEFAULT_CSV_PATH_TS = '../../../Data/csv/tushare'
DEFAULT_MY_SQL_PATH_TS = '../../../Data/mysql/tushare'
DEFAULT_MONGO_DB_PATH_TS = '../../../Data/mongodb/tushare'


def get_directory_path():
    dir_path = ''
    if STORAGE_MODE == USING_CSV:
        dir_path = '%s/security_trade_data/trade/current_day' % DEFAULT_CSV_PATH_TS
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    return dir_path


def get_all_security_current_day_data():
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
    if STORAGE_MODE == USING_CSV:
        dm_logger().debug('get_all_security_current_day_data begin...')
        file_path = '%s/trade.csv' % get_directory_path()
        try:
            dm_logger().debug('tushare.get_today_all()')
            df = tushare.get_today_all()
        except Exception as exception:
            dm_logger().error('tushare.get_today_all() excpetion, args: %s' % exception.args.__str__())
        else:
            if df is None:
                dm_logger().warning('tushare.get_today_all() return none')
            else:
                df.to_csv(file_path)
            dm_logger().debug('get_all_security_current_day_data Finish...')


if __name__ == "__main__":
    get_all_security_current_day_data()

