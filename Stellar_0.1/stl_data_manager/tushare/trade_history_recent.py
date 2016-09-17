# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import os
import datetime
import linecache
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import tushare

from stl_utils.logger import dm_log
from stl_utils.common import get_stellar_root
from stl_utils import file as sfu
from stl_data_manager.tushare import fundamental as sfund


'''
获取证券股票的近期行情信息
'''


# Global Consts
USING_CSV = 1
USING_MY_SQL = 2
USING_MONGO_DB = 3
STORAGE_MODE = USING_CSV

# TuShare Data Storage Path
DEFAULT_CSV_PATH_TS = ('%s/Data/csv/tushare' % get_stellar_root())
DEFAULT_MY_SQL_PATH_TS = ('%s/Data/mysql/tushare' % get_stellar_root())
DEFAULT_MONGO_DB_PATH_TS = ('%s/Data/mongodb/tushare' % get_stellar_root())

THREAD_COUNT = 50    # 查询交易数据的并发线程数
RETRY_COUNT = 5      # 调用tushare接口失败重试次数
RETRY_PAUSE = 0.1    # 调用tushare接口失败重试间隔时间
DROP_FACTOR = True   # 是否移除复权因子，在分析过程中可能复权因子意义不大，但是如需要先储存到数据库之后再分析的话，有该项目会更加灵活


def get_directory_path():
    dir_path = ''
    if STORAGE_MODE == USING_CSV:
        dir_path = '%s/security_trade_data/trade/history/recent' % DEFAULT_CSV_PATH_TS
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    return dir_path


def get_csv_path(code, type):
    dir_path = '.'
    if type == 'D':
        dir_path = '%s/day' % get_directory_path()
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    elif type == 'W':
        dir_path = '%s/week' % get_directory_path()
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    elif type == 'M':
        dir_path = '%s/month' % get_directory_path()
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    elif type == '5':
        dir_path = '%s/5min' % get_directory_path()
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    elif type == '15':
        dir_path = '%s/15min' % get_directory_path()
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    elif type == '30':
        dir_path = '%s/30min' % get_directory_path()
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    elif type == '60':
        dir_path = '%s/60min' % get_directory_path()
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

    file_path = '%s/%s.csv' % (dir_path, code)
    return file_path


def get_all_security_recent_data_no_multi_thread():
    '''
    获取所有股票历史行情, 并存入到对应的csv文件中, 不使用多线程

    Parameters
    ------
        无
    return
    -------
        无
    '''
    dm_log.debug('get_all_security_history_no_multi_thread Begin...')

    code_list = sfund.get_all_security_basic_info()
    for code in code_list:
        get_recent_data(code, 'D')    # 获取最近3年所有股票的日线数据
        get_recent_data(code, 'W')    # 获取最近3年所有股票的周线数据
        get_recent_data(code, 'M')    # 获取最近3年所有股票的月线数据
        get_recent_data(code, '5')    # 获取最近3年所有股票的5分钟线数据
        get_recent_data(code, '15')   # 获取最近3年所有股票的15分钟线数据
        get_recent_data(code, '30')   # 获取最近3年所有股票的30分钟线数据
        get_recent_data(code, '60')   # 获取最近3年所有股票的60分钟线数据

    dm_log.debug('get_all_security_history_no_multi_thread Finish...')


def get_all_security_recent_data_multi_thread():
    '''
    获取所有股票近期行情, 并存入到对应的csv文件中, 使用多线程

    Parameters
    ------
        无
    return
    -------
        无
    '''
    dm_log.debug('get_all_security_recent_data_multi_thread (%d threads) Begin...' % THREAD_COUNT)

    code_list = sfund.get_all_security_basic_info()              # 获取所有股票的基本信息
    get_recent_data_multi_thread(code_list, 'D', THREAD_COUNT)   # 获取最近3年所有股票的日线数据
    get_recent_data_multi_thread(code_list, 'W', THREAD_COUNT)   # 获取最近3年所有股票的周线数据
    get_recent_data_multi_thread(code_list, 'M', THREAD_COUNT)   # 获取最近3年所有股票的月线数据
    get_recent_data_multi_thread(code_list, '5', THREAD_COUNT)   # 获取最近3年所有股票的5分钟线数据
    get_recent_data_multi_thread(code_list, '15', THREAD_COUNT)  # 获取最近3年所有股票的15分钟线数据
    get_recent_data_multi_thread(code_list, '30', THREAD_COUNT)  # 获取最近3年所有股票的30分钟线数据
    get_recent_data_multi_thread(code_list, '60', THREAD_COUNT)  # 获取最近3年所有股票的60分钟线数据

    dm_log.debug('get_all_security_recent_data_multi_thread (%d threads)Finish...' % THREAD_COUNT)


def get_recent_data_multi_thread(code_list, type, thread_count):
    '''
    获取code对应股票的近3年历史行情信息,并将结果保存到对应csv文件

    Parameters
    ------
        code_list: 股票代码列表
        type: 数据类型：D=日k线 W=周 M=月 5=5分钟 15=15分钟 30=30分钟 60=60分钟
        thread_count:线程数
    return
    -------
        无
    '''
    callable_name = get_recent_day_data
    if type == 'D':
        callable_name = get_recent_day_data
    elif type == 'W':
        callable_name = get_recent_week_data
    elif type == 'M':
        callable_name = get_recent_month_data
    elif type == '5':
        callable_name = get_recent_5min_data
    elif type == '15':
        callable_name = get_recent_15min_data
    elif type == '30':
        callable_name = get_recent_30min_data
    elif type == '60':
        callable_name = get_recent_60min_data

    pool = ThreadPoolExecutor(max_workers=thread_count)
    pool.map(callable_name, code_list)


def get_recent_day_data(code):
    return get_recent_data(code, 'D')

def get_recent_week_data(code):
    return get_recent_data(code, 'W')

def get_recent_month_data(code):
    return get_recent_data(code, 'M')

def get_recent_5min_data(code):
    return get_recent_data(code, '5')

def get_recent_15min_data(code):
    return get_recent_data(code, '15')

def get_recent_30min_data(code):
    return get_recent_data(code, '30')

def get_recent_60min_data(code):
    return get_recent_data(code, '60')


def get_recent_data(code, type):
    '''
    获取code对应股票的近3年历史行情信息,并将结果保存到对应csv文件

    tushare.get_hist_data()查询指定股票3年的历史行情, 返回数据如下:
        date：日期
        open：开盘价
        high：最高价
        close：收盘价
        low：最低价
        volume：成交量
        price_change：价格变动
        p_change：涨跌幅
        ma5：5日均价
        ma10：10日均价
        ma20:20日均价
        v_ma5:5日均量
        v_ma10:10日均量
        v_ma20:20日均量
        turnover:换手率[注：指数无此项]

    Parameters
    ------
        code: 股票代码
        type: 数据类型：D=日k线 W=周 M=月 5=5分钟 15=15分钟 30=30分钟 60=60分钟
    return
    -------
        无
    '''
    if STORAGE_MODE == USING_CSV:
        file_path = get_csv_path(code, type)
        (is_update, start_date_str, end_date_str) = get_input_para(file_path)
        if start_date_str == end_date_str:
            dm_log.debug('%s data is already up-to-date.' % file_path)
        else:
            try:
                dm_log.debug('tushare.get_hist_data: %s, %s, start=%s, end=%s called' % (type, code, start_date_str, end_date_str))
                df = tushare.get_hist_data(code, start=start_date_str, end=end_date_str, ktype=type, retry_count=RETRY_COUNT, pause=RETRY_PAUSE)
            except Exception as exception:
                dm_log.error('tushare.get_hist_data(%s) excpetion, args: %s' % (code, exception.args.__str__()))
            else:
                if df is None:
                    dm_log.warning('tushare.get_hist_data(%s) return none' % code)
                else:
                    dm_log.debug('tushare.get_hist_data: %s, %s, start=%s, end=%s done, get %d rows' % (type, code, start_date_str, end_date_str, len(df)))
                    if is_update:
                        old_data = pd.read_csv(file_path, index_col=0)
                        all_data = df.append(old_data)
                        all_data.to_csv(file_path)
                    else:
                        df.to_csv(file_path)


def get_input_para(file_path):
    '''
    获取指定csv文件的中最新一条记录的信息，返回需要获取信息的起始日期，以及是更新还是新建

    Parameters
    ------
        file_path: 指定文件路径
    return
    -------
        is_update: 更新还是新建，True：更新，False：新建
        start_date_str: 本次获取数据的开始日期
        end_date_str: 本次获取数据的结束日期
    '''
    start_date_str = '2000-01-01'
    end_date_str = datetime.date.strftime(datetime.date.today(), '%Y-%m-%d')
    is_update = False
    if os.path.exists(file_path):
        dm_log.debug('%s exists, do update task' % file_path)
        line = linecache.getline(file_path, 2)
        if line is None:
            is_update = False
        elif line == '':
            is_update = False
        elif line == '\n':
            is_update = False
        else:
            is_update = True
            date_str = line[0:10]
            latest_date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
            today = datetime.datetime.today()
            if (today - latest_date).days >= 1:
                next_day = latest_date + datetime.timedelta(days=1)
                start_date_str = datetime.date.strftime(next_day.date(), '%Y-%m-%d')
            else:
                start_date_str = end_date_str
    else:
        dm_log.debug('%s does not exist, do get all task' % file_path)

    return (is_update, start_date_str, end_date_str)


def print_result(request, result):
    print("---Result from request %s : %r" % (request.requestID, result))


def check_data_integrity(data_path):
    '''
    对比data_path对应的文件夹中所有csv文件和basic_info.csv中code是否对应

    Parameters
    ------
        data_path: 指定文件夹路径
    return
    -------
        missing_code_list: 缺失的code列表
    '''
    data_code_list = sfu.get_code_list_in_dir(data_path)
    try:
        basic_data = tushare.get_stock_basics()
    except Exception as exception:
        dm_log.error('tushare.get_stock_basics() excpetion, args: %s' % exception.args.__str__())
    else:
        if basic_data is None:
            dm_log.warning('tushare.get_stock_basics() return none')
            return []
        else:
            missing_code_list = []
            code_list = basic_data.index
            for code in code_list:
                found = False
                for tmp_code in data_code_list:
                    if tmp_code == code:
                        found = True
                        break
                if found is not True:
                    missing_code_list.append(code)

            return missing_code_list


if __name__ == "__main__":
    get_all_security_recent_data_multi_thread()
    # get_all_security_recent_data_no_multi_thread()






