# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


from stl_utils import stl_logger as slog
from stl_utils import stl_thread_pool as stp
from stl_data_manager.tushare import stl_dm_fundamental as sfund

import os
import datetime
import time
import pandas as pd
import tushare


'''
获取证券股票的指定交易日的分笔(TICK)行情信息
'''


THREAD_COUNT = 50    # 查询交易数据的并行线程数
RETRY_COUNT = 5      # 调用tushare接口失败重试次数
RETRY_PAUSE = 0.1    # 调用tushare接口失败重试间隔时间

TICK_FORWARD = 0
TICK_BACKWARD = 1

DEFAULT_DIR_PATH = '../../../Data/origin/tushare/security_trade_data/tick/history'


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
    slog.StlDmLogger().debug('get_all_security_tick_data_no_multi_thread Begin...')

    code_list = sfund.get_all_security_basic_info()              # 获取所有股票的基本信息
    tick_date_str = start_date_str
    for offset in range(1, during):
        for code in code_list:
            slog.StlDmLogger().debug('get_tick_data, code: %s, tick_date: %s' % (code, tick_date_str))
            get_tick_data(code, tick_date_str)
        if direction == TICK_BACKWARD:
            tick_step = -1
        else:
            tick_step = 1
        print(tick_date_str)
        star_date = datetime.datetime.strptime(tick_date_str, '%Y-%m-%d')
        next_day = star_date + datetime.timedelta(days=tick_step)
        tick_date_str = datetime.datetime.strftime(next_day, '%Y-%m-%d')

    slog.StlDmLogger().debug('get_all_security_tick_data_no_multi_thread Finish...')


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
            slog.StlDmLogger().debug('get_tick_data, code: %s, tick_date: %s' % (code, tick_date_str))
            req = stp.StlWorkRequest(get_tick_data, args=[code, tick_date_str], callback=print_result)
            sh_thread_pool.putRequest(req)
            slog.StlDmLogger().debug('work request #%s added to sh_thread_pool' % req.requestID)
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
            slog.StlDmLogger().debug('No Pending Results')
            break
    sh_thread_pool.stop()


def print_result(request, result):
    print("---Result from request %s : %r" % (request.requestID, result))


def get_tick_data(code, tick_date):
    '''
    获取code对应股票在指定时间长度内的分笔交易信息

    Parameters
    ------
        code: 股票代码
        tick_date: 查询日期
    return
    -------
        无
    '''
    tick_dir_path = '%s/%s' % (DEFAULT_DIR_PATH, tick_date)
    if not os.path.exists(tick_dir_path):
        os.makedirs(tick_dir_path)

    file_path = '%s/%s.csv' % (tick_dir_path, code)
    if not os.path.exists(file_path):
        try:
            tmp_data = pd.DataFrame()
            slog.StlDmLogger().debug('tushare.get_tick_data: %s, tick_date=%s' % (code, tick_date))
            tmp_data = tushare.get_tick_data(code, tick_date, retry_count=RETRY_COUNT, pause=RETRY_PAUSE)
        except Exception as exception:
            slog.StlDmLogger().error('tushare.get_tick_data(%s) excpetion, args: %s' % (code, exception.args.__str__()))

        if tmp_data is None:
            slog.StlDmLogger().warning('tushare.get_tick_data(%s) return none' % code)
        else:
            tmp_data.to_csv(file_path)
    else:
        slog.StlDmLogger().debug('%s already exists' % file_path)


if __name__ == "__main__":
    today = datetime.datetime.today()
    start_date_str = datetime.datetime.strftime(today, '%Y-%m-%d')
    get_all_security_tick_data_multi_thread(start_date_str=start_date_str, during=10, direction=TICK_BACKWARD)
    # get_all_security_tick_data_no_multi_thread(start_date_str=start_date_str, during=10, direction=TICK_BACKWARD)
    # get_tick_data('002612', today)


