# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import os

import tushare

from code.utils.logger import dm_log
from code.utils.common import get_stellar_root


'''
获取银行间同业拆借利率数据
'''


# Global Consts
USING_CSV = 1
USING_MY_SQL = 2
USING_MONGO_DB = 3
STORAGE_MODE = USING_CSV

# TuShare Data Storage Path
DEFAULT_CSV_PATH_TS = ('%s/Data/csv/tushare' % get_stellar_root())
DEFAULT_MY_SQL_PATH_TS = ('../../../Data/mysql/tushare' % get_stellar_root())
DEFAULT_MONGO_DB_PATH_TS = ('../../../Data/mongodb/tushare' % get_stellar_root())


DATA_YEAR = 2016              # 获取数据的年份
DATA_QUARTER = 1              # 获取数据的季度
DATA_MONTH = 1                # 获取数据的月份

RETRY_COUNT = 5               # 调用tushare接口失败重试次数
RETRY_PAUSE = 0.1             # 调用tushare接口失败重试间隔时间


def get_directory_path():
    dir_path = ''
    if STORAGE_MODE == USING_CSV:
        dir_path = '%s/shibor_data' % DEFAULT_CSV_PATH_TS
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    return dir_path


def get_shibor_data(year):
    '''
    获取上海银行间同业拆借利率(Shibor)

        date:日期
        ON:隔夜拆放利率
        1W:1周拆放利率
        2W:2周拆放利率
        1M:1个月拆放利率
        3M:3个月拆放利率
        6M:6个月拆放利率
        9M:9个月拆放利率
        1Y:1年拆放利率

    Parameters
    ------
        year: 年份
    return
    -------
        无
    '''
    try:
        dm_log.debug('tushare.shibor_data(year=%s) called' % year)
        df = tushare.shibor_data(year=year)
    except Exception as exception:
        dm_log.error('tushare.shibor_data(%s) excpetion, args: %s' % (year, exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.shibor_data(%s) return none' % year)
        else:
            dm_log.debug('tushare.shibor_data(year=%s) done, got %d rows' % (year, len(df)))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/shibor/%d.csv' % (get_directory_path(), year)
                df.to_csv(file_path)


def get_shibor_quote_data(year):
    '''
    获取Shibor银行报价数据

        date:日期
        bank:报价银行名称
        ON:隔夜拆放利率
        ON_B:隔夜拆放买入价
        ON_A:隔夜拆放卖出价
        1W_B:1周买入
        1W_A:1周卖出
        2W_B:买入
        2W_A:卖出
        1M_B:买入
        1M_A:卖出
        3M_B:买入
        3M_A:卖出
        6M_B:买入
        6M_A:卖出
        9M_B:买入
        9M_A:卖出
        1Y_B:买入
        1Y_A:卖出

    Parameters
    ------
        year: 年份
    return
    -------
        无
    '''
    try:
        dm_log.debug('tushare.shibor_quote_data(year=%s) called' % year)
        df = tushare.shibor_quote_data(year=year)
    except Exception as exception:
        dm_log.error('tushare.shibor_quote_data(%s) excpetion, args: %s' % (year, exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.shibor_quote_data(%s) return none' % year)
        else:
            dm_log.debug('tushare.shibor_quote_data(year=%s) done, got %d rows' % (year, len(df)))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/shibor_quote/%d.csv' % (get_directory_path(), year)
                df.to_csv(file_path)


def get_shibor_ma_data(year):
    '''
    获取Shibor均值数据

        date, 日期
        ON_5, 隔夜拆放利率5周期均值
        ON_10, 隔夜拆放利率10周期均值
        ON_20, 隔夜拆放利率20周期均值
        1W_5, 1周拆放利率5周期均值
        1W_10, 1周拆放利率10周期均值
        1W_20, 1周拆放利率20周期均值
        2W_5, 2周拆放利率5周期均值
        2W_10, 2周拆放利率10周期均值
        2W_20, 2周拆放利率20周期均值
        1M_5, 1月拆放利率5周期均值
        1M_10, 1月拆放利率10周期均值
        1M_20, 1月拆放利率20周期均值
        3M_5, 3月拆放利率5周期均值
        3M_10, 3月拆放利率10周期均值
        3M_20, 3月拆放利率20周期均值
        6M_5, 6月拆放利率5周期均值
        6M_10, 6月拆放利率10周期均值
        6M_20, 6月拆放利率20周期均值
        9M_5, 9月拆放利率5周期均值
        9M_10, 9月拆放利率10周期均值
        9M_20, 9月拆放利率20周期均值
        1Y_5, 1年拆放利率5周期均值
        1Y_10, 1年拆放利率10周期均值
        1Y_20, 1年拆放利率20周期均值

    Parameters
    ------
        year: 年份
    return
    -------
        无
    '''
    try:
        dm_log.debug('tushare.shibor_ma_data(year=%s) called' % year)
        df = tushare.shibor_ma_data(year=year)
    except Exception as exception:
        dm_log.error('tushare.shibor_ma_data(%s) excpetion, args: %s' % (year, exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.shibor_ma_data(%s) return none' % year)
        else:
            dm_log.debug('tushare.shibor_ma_data(year=%s) done, got %d rows' % (year, len(df)))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/shibor_ma/%d.csv' % (get_directory_path(), year)
                df.to_csv(file_path)


def get_lpr_data(year):
    '''
    获取贷款基础利率（LPR）

        date:日期
        1Y:1年贷款基础利率

    Parameters
    ------
        year: 年份
    return
    -------
        无
    '''
    try:
        dm_log.debug('tushare.lpr_data(year=%s) called' % year)
        df = tushare.lpr_data(year=year)
    except Exception as exception:
        dm_log.error('tushare.lpr_data(%s) excpetion, args: %s' % (year, exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.lpr_data(%s) return none' % year)
        else:
            dm_log.debug('tushare.lpr_data(year=%s) done, got %d rows' % (year, len(df)))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/lpr/%d.csv' % (get_directory_path(), year)
                df.to_csv(file_path)


def get_lpr_ma_data(year):
    '''
    获取贷款基础利率均值数据

        date:日期
        1Y:1年贷款基础利率

    Parameters
    ------
        year: 年份
    return
    -------
        无
    '''
    try:
        dm_log.debug('tushare.lpr_ma_data(year=%s) called' % year)
        df = tushare.lpr_ma_data(year=year)
    except Exception as exception:
        dm_log.error('tushare.lpr_ma_data(%s) excpetion, args: %s' % (year, exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.lpr_ma_data(%s) return none' % year)
        else:
            dm_log.debug('tushare.lpr_ma_data(year=%s) done, got %d rows' % (year, len(df)))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/lpr_ma/%d.csv' % (get_directory_path(), year)
                df.to_csv(file_path)


if __name__ == '__main__':
    get_shibor_data(2016)
    get_shibor_quote_data(2016)
    get_shibor_ma_data(2016)
    get_lpr_data(2016)
    get_lpr_ma_data(2016)
