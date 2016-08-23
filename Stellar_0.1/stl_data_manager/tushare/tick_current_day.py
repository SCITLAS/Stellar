# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import os
from concurrent.futures import ThreadPoolExecutor

import tushare

from stl_utils.logger import dm_log
from stl_utils.data_utils import is_market_closed
from stl_utils.data_utils import need_data_file_refresh
from stl_data_manager.tushare import fundamental as sfund


'''
获取证券股票最新一个交易日的分笔(TICK)行情信息
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

THREAD_COUNT = 100    # 查询交易数据的并行线程数
RETRY_COUNT = 5      # 调用tushare接口失败重试次数
RETRY_PAUSE = 0.1    # 调用tushare接口失败重试间隔时间

TICK_FORWARD = 0
TICK_BACKWARD = 1


def get_directory_path():
    dir_path = ''
    if STORAGE_MODE == USING_CSV:
        dir_path = '%s/security_trade_data/tick/current_day' % DEFAULT_CSV_PATH_TS
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    return dir_path


def get_all_security_current_day_tick_data_no_multi_thread():
    '''
    获取所有股票在最新一个交易日的分笔交易信息, 非多线程版本

    Parameters
    ------
        无
    return
    -------
        无
    '''
    dm_log.debug('get_all_security_current_day_tick_data_no_multi_thread Begin...')

    code_list = sfund.get_all_security_basic_info()              # 获取所有股票的基本信息
    for code in code_list:
        dm_log.debug('get_current_day_tick_data, code: %s' % code)
        get_current_day_tick_data(code)

    dm_log.debug('get_all_security_current_day_tick_data_no_multi_thread Finish...')


def get_all_security_current_day_tick_data_multi_thread():
    '''
    获取所有股票最新一个交易日的分笔交易信息, 多线程版本

    Parameters
    ------
        无
    return
    -------
        无
    '''
    code_list = sfund.get_all_security_basic_info()              # 获取所有股票的基本信息
    pool = ThreadPoolExecutor(max_workers=THREAD_COUNT)
    pool.map(get_current_day_tick_data, code_list)


def print_result(request, result):
    print("---Result from request %s : %r" % (request.requestID, result))


def get_current_day_tick_data(code):
    '''
    获取code对应股票最新一个交易日的分笔交易信息

        time：时间
        price：当前价格
        pchange:涨跌幅
        change：价格变动
        volume：成交手
        amount：成交金额(元)
        type：买卖类型【买盘、卖盘、中性盘】

    Parameters
    ------
        code: 股票代码
    return
    -------
        无
    '''
    if not is_market_closed():
        # now is too early for refresh, current day data is not ready.
        dm_log.debug('Now is too early for refresh, current day data is not ready.')
        return

    if STORAGE_MODE == USING_CSV:
        file_path = '%s/%s.csv' % (get_directory_path(), code)

        if not need_data_file_refresh(file_path):
            # the file is already refreshed some time current day after valid date, no need to refresh
            dm_log.debug('%s is already up-to-date, no need to refresh.' % file_path)
            return

        try:
            dm_log.debug('tushare.get_today_ticks: %s called' % code)
            df = tushare.get_today_ticks(code, retry_count=RETRY_COUNT, pause=RETRY_PAUSE)
        except Exception as exception:
            dm_log.error('tushare.get_today_ticks(%s) excpetion, args: %s' % (code, exception.args.__str__()))
        else:
            if df is None:
                dm_log.warning('tushare.get_today_ticks(%s) return none' % code)
            else:
                dm_log.debug('tushare.get_today_ticks: %s done, got %d rows' % (code, len(df)))
                df.to_csv(file_path)



if __name__ == "__main__":
    get_all_security_current_day_tick_data_multi_thread()
    # get_all_security_current_day_tick_data_no_multi_thread()
    # get_current_day_tick_data('002612')
    # get_current_day_tick_data('100005')


