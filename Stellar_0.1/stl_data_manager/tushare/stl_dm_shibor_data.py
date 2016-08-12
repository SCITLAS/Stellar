# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


from sqlalchemy import create_engine
from stl_utils import stl_logger as slog
import tushare
import os
import pandas as pd


'''
获取银行间同业拆借利率数据
'''


# Global Consts
USING_H5 = 0
USING_CSV = 1
USING_MY_SQL = 2
USING_MONGO_DB = 3
STORAGE_MODE = USING_H5

# TuShare Data Storage Path
DEFAULT_H5_PATH_TS = '../../../Data/h5/tushare'
DEFAULT_CSV_PATH_TS = '../../../Data/csv/tushare'
DEFAULT_MY_SQL_PATH_TS = '../../../Data/mysql/tushare'
DEFAULT_MONGO_DB_PATH_TS = '../../../Data/mongodb/tushare'


DATA_YEAR = 2016              # 获取数据的年份
DATA_QUARTER = 1              # 获取数据的季度
DATA_MONTH = 1                # 获取数据的月份

RETRY_COUNT = 5               # 调用tushare接口失败重试次数
RETRY_PAUSE = 0.1             # 调用tushare接口失败重试间隔时间


def get_directory_path():
    dir_path = ''
    if STORAGE_MODE == USING_H5:
        dir_path = '%s/shibor_data' % DEFAULT_H5_PATH_TS
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    elif STORAGE_MODE == USING_CSV:
        dir_path = '%s/shibor_data' % DEFAULT_CSV_PATH_TS
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    elif STORAGE_MODE == USING_MY_SQL:
        dir_path = '%s/shibor_data' % DEFAULT_MY_SQL_PATH_TS
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
        slog.StlDmLogger().debug('tushare.shibor_data(year=%s)' % year)
        df = tushare.shibor_data(year=year)
    except Exception as exception:
        slog.StlDmLogger().error('tushare.shibor_data(%s) excpetion, args: %s' % (year, exception.args.__str__()))
        return

    if df is None:
        slog.StlDmLogger().warning('tushare.shibor_data(%s) return none' % year)
    else:
        slog.StlDmLogger().debug('shibor_%d: %d' % (year, len(df)))
        if STORAGE_MODE == USING_H5:
            file_path = '%s/shibor.h5' % get_directory_path()
            store = pd.HDFStore(path=file_path, mode='a', append=True)
            node_path = '/shibor_%d' % year
            node = store.get_node(key=node_path)
            if node is None:
                slog.StlDmLogger().debug('%s do not exist, create a new one' % (node_path))
            else:
                slog.StlDmLogger().debug('%s do exist, delete old one' % (node_path))
                store.remove(key=node_path)
            store[node_path] = df
            store.flush()
            store.close()
        elif STORAGE_MODE == USING_CSV:
            file_path = '%s/shibor_%d.csv' % (get_directory_path(), year)
            df.to_csv(file_path)
        elif STORAGE_MODE == USING_MY_SQL:
            engine = create_engine('mysql://user:passwd@127.0.0.1/db_name?charset=utf8')



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
        slog.StlDmLogger().debug('tushare.shibor_quote_data(year=%s)' % year)
        df = tushare.shibor_quote_data(year=year)
    except Exception as exception:
        slog.StlDmLogger().error('tushare.shibor_quote_data(%s) excpetion, args: %s' % (year, exception.args.__str__()))
        return

    if df is None:
        slog.StlDmLogger().warning('tushare.shibor_quote_data(%s) return none' % year)
    else:
        slog.StlDmLogger().debug('shibor_quote_%d: %d' % (year, len(df)))
        if STORAGE_MODE == USING_H5:
            file_path = '%s/shibor_quote.h5' % get_directory_path()
            store = pd.HDFStore(path=file_path, mode='a', append=True)
            node_path = '/shibor_quote_%d' % year
            node = store.get_node(key=node_path)
            if node is None:
                slog.StlDmLogger().debug('%s do not exist, create a new one' % (node_path))
            else:
                slog.StlDmLogger().debug('%s do exist, delete old one' % (node_path))
                store.remove(key=node_path)
            store[node_path] = df
            store.flush()
            store.close()
        elif STORAGE_MODE == USING_CSV:
            file_path = '%s/shibor_quote_%d.csv' % (get_directory_path(), year)
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
        slog.StlDmLogger().debug('tushare.shibor_ma_data(year=%s)' % year)
        df = tushare.shibor_ma_data(year=year)
    except Exception as exception:
        slog.StlDmLogger().error('tushare.shibor_ma_data(%s) excpetion, args: %s' % (year, exception.args.__str__()))
        return

    if df is None:
        slog.StlDmLogger().warning('tushare.shibor_ma_data(%s) return none' % year)
    else:
        slog.StlDmLogger().debug('shibor_ma_%d: %d' % (year, len(df)))
        if STORAGE_MODE == USING_H5:
            file_path = '%s/shibor_ma.h5' % get_directory_path()
            store = pd.HDFStore(path=file_path, mode='a', append=True)
            node_path = '/shibor_ma_%d' % year
            node = store.get_node(key=node_path)
            if node is None:
                slog.StlDmLogger().debug('%s do not exist, create a new one' % (node_path))
            else:
                slog.StlDmLogger().debug('%s do exist, delete old one' % (node_path))
                store.remove(key=node_path)
            store[node_path] = df
            store.flush()
            store.close()
        elif STORAGE_MODE == USING_CSV:
            file_path = '%s/shibor_ma_%d.csv' % (get_directory_path(), year)
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
        slog.StlDmLogger().debug('tushare.lpr_data(year=%s)' % year)
        df = tushare.lpr_data(year=year)
    except Exception as exception:
        slog.StlDmLogger().error('tushare.lpr_data(%s) excpetion, args: %s' % (year, exception.args.__str__()))
        return

    if df is None:
        slog.StlDmLogger().warning('tushare.lpr_data(%s) return none' % year)
    else:
        slog.StlDmLogger().debug('lpr-%d: %d' % (year, len(df)))
        if STORAGE_MODE == USING_H5:
            file_path = '%s/lpr.h5' % get_directory_path()
            store = pd.HDFStore(path=file_path, mode='a', append=True)
            node_path = '/lpr_%d' % year
            node = store.get_node(key=node_path)
            if node is None:
                slog.StlDmLogger().debug('%s do not exist, create a new one' % (node_path))
            else:
                slog.StlDmLogger().debug('%s do exist, delete old one' % (node_path))
                store.remove(key=node_path)
            store[node_path] = df
            store.flush()
            store.close()
        elif STORAGE_MODE == USING_CSV:
            file_path = '%s/lpr_%d.csv' % (get_directory_path(), year)
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
        slog.StlDmLogger().debug('tushare.lpr_ma_data(year=%s)' % year)
        df = tushare.lpr_ma_data(year=year)
    except Exception as exception:
        slog.StlDmLogger().error('tushare.lpr_ma_data(%s) excpetion, args: %s' % (year, exception.args.__str__()))
        return

    if df is None:
        slog.StlDmLogger().warning('tushare.lpr_ma_data(%s) return none' % year)
    else:
        slog.StlDmLogger().debug('lpr_ma-%d: %d' % (year, len(df)))
        if STORAGE_MODE == USING_H5:
            file_path = '%s/lpr_ma.h5' % get_directory_path()
            store = pd.HDFStore(path=file_path, mode='a', append=True)
            node_path = '/lpr_ma_%d' % year
            node = store.get_node(key=node_path)
            if node is None:
                slog.StlDmLogger().debug('%s do not exist, create a new one' % (node_path))
            else:
                slog.StlDmLogger().debug('%s do exist, delete old one' % (node_path))
                store.remove(key=node_path)
            store[node_path] = df
            store.flush()
            store.close()
        elif STORAGE_MODE == USING_CSV:
            file_path = '%s/lpr_ma_%d.csv' % (get_directory_path(), year)
            df.to_csv(file_path)


if __name__ == '__main__':
    get_shibor_data(2016)
    # get_shibor_quote_data(2016)
    # get_shibor_ma_data(2016)
    # get_lpr_data(2016)
    # get_lpr_ma_data(2016)
