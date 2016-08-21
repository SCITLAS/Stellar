# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import os
import datetime
import time

import tushare

from stl_utils.logger import dm_logger
from stl_utils import thread_pool as stp
from stl_data_manager.tushare import fundamental as sfund


'''
获取证券股票的指定交易日的分笔(TICK)行情信息
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

THREAD_COUNT = 50    # 查询交易数据的并行线程数
RETRY_COUNT = 5      # 调用tushare接口失败重试次数
RETRY_PAUSE = 0.1    # 调用tushare接口失败重试间隔时间

TICK_FORWARD = 0
TICK_BACKWARD = 1


def get_directory_path():
    dir_path = ''
    if STORAGE_MODE == USING_CSV:
        dir_path = '%s/security_trade_data/tick/history' % DEFAULT_CSV_PATH_TS
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    return dir_path


def get_all_security_tick_data_no_multi_thread(start_date_str, during, direction):
    '''
    获取所有股票在指定时间长度内的分笔交易信息, 非多线程版本

    Parameters
    ------
        start_date: 查询开始日期
        during: 自start_date开始持续天数
        direction: 向start_date前还是向后查, 0:向前, 1:向后
    return
    -------
        无
    '''
    dm_logger().debug('get_all_security_tick_data_no_multi_thread Begin...')

    code_list = sfund.get_all_security_basic_info()              # 获取所有股票的基本信息
    tick_date_str = start_date_str
    for offset in range(1, during):
        for code in code_list:
            dm_logger().debug('get_tick_data, code: %s, tick_date: %s' % (code, tick_date_str))
            get_tick_data(code, tick_date_str)
        if direction == TICK_BACKWARD:
            tick_step = -1
        else:
            tick_step = 1
        print(tick_date_str)
        star_date = datetime.datetime.strptime(tick_date_str, '%Y-%m-%d')
        next_day = star_date + datetime.timedelta(days=tick_step)
        tick_date_str = datetime.datetime.strftime(next_day, '%Y-%m-%d')

    dm_logger().debug('get_all_security_tick_data_no_multi_thread Finish...')


def get_all_security_tick_data_multi_thread(start_date_str, during, direction):
    '''
    获取所有股票在指定时间长度内的分笔交易信息, 多线程版本

    Parameters
    ------
        start_date: 查询开始日期
        during: 自start_date开始持续天数
        direction: 向start_date前还是向后查, 0:向前, 1:向后
    return
    -------
        无
    '''
    code_list = sfund.get_all_security_basic_info()              # 获取所有股票的基本信息
    tick_date_str = start_date_str

    sh_thread_pool = stp.StlThreadPool(THREAD_COUNT)
    for offset in range(1, during):
        for code in code_list:
            dm_logger().debug('get_tick_data, code: %s, tick_date: %s' % (code, tick_date_str))
            req = stp.StlWorkRequest(get_tick_data, args=[code, tick_date_str], callback=print_result)
            sh_thread_pool.putRequest(req)
            dm_logger().debug('work request #%s added to sh_thread_pool' % req.requestID)
        if direction == TICK_BACKWARD:
            tick_step = -1
        else:
            tick_step = 1
        print(tick_date_str)
        star_date = datetime.datetime.strptime(tick_date_str, '%Y-%m-%d')
        next_day = star_date + datetime.timedelta(days=tick_step)
        tick_date_str = datetime.datetime.strftime(next_day, '%Y-%m-%d')

    while True:
        try:
            time.sleep(0.5)
            sh_thread_pool.poll()
        except stp.StlNoResultsPendingException:
            dm_logger().debug('No Pending Results')
            break
    sh_thread_pool.stop()


def print_result(request, result):
    print("---Result from request %s : %r" % (request.requestID, result))


def get_tick_data(code, tick_date):
    '''
    获取code对应股票在指定时间长度内的分笔交易信息

        time：时间
        price：成交价格
        change：价格变动
        volume：成交手
        amount：成交金额(元)
        type：买卖类型【买盘、卖盘、中性盘】

    Parameters
    ------
        code: 股票代码
        tick_date: 查询日期
    return
    -------
        无
    '''
    if STORAGE_MODE == USING_CSV:
        file_path = '%s/%s/%s.csv' % (get_directory_path(), tick_date, code)
        if not os.path.exists(file_path):
            try:
                dm_logger().debug('tushare.get_tick_data: %s, tick_date=%s' % (code, tick_date))
                df = tushare.get_tick_data(code, tick_date, retry_count=RETRY_COUNT, pause=RETRY_PAUSE)
            except Exception as exception:
                dm_logger().error('tushare.get_tick_data(%s) excpetion, args: %s' % (code, exception.args.__str__()))
            else:
                if df is None:
                    dm_logger().warning('tushare.get_tick_data(%s) return none' % code)
                else:
                    df.to_csv(file_path)
        else:
            dm_logger().debug('%s already exists' % file_path)


if __name__ == "__main__":
    today = datetime.datetime.today()
    start_date_str = datetime.datetime.strftime(today, '%Y-%m-%d')
    get_all_security_tick_data_multi_thread(start_date_str=start_date_str, during=10, direction=TICK_BACKWARD)
    # get_all_security_tick_data_no_multi_thread(start_date_str=start_date_str, during=10, direction=TICK_BACKWARD)
    # get_tick_data('002612', today)


