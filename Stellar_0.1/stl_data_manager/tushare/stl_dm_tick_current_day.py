# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


from stl_utils import stl_logger as slog
from stl_utils import stl_thread_pool as stp
from stl_data_manager.tushare import stl_dm_fundamental as sfund

import os
import time
import pandas as pd
import tushare


'''
获取证券股票最新一个交易日的分笔(TICK)行情信息
存入对应的xlsx文件
'''


THREAD_COUNT = 50    # 查询交易数据的并行线程数
RETRY_COUNT = 5      # 调用tushare接口失败重试次数
RETRY_PAUSE = 0.1    # 调用tushare接口失败重试间隔时间

TICK_FORWARD = 0
TICK_BACKWARD = 1

DEFAULT_DIR_PATH = '../../../Data/origin/tushare/security_trade_data/tick/current_day'


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
    slog.StlDmLogger().debug('get_all_security_current_day_tick_data_no_multi_thread Begin...')

    code_list = sfund.get_all_security_basic_info()              # 获取所有股票的基本信息
    for code in code_list:
        slog.StlDmLogger().debug('get_current_day_tick_data, code: %s' % code)
        get_current_day_tick_data(code)

    slog.StlDmLogger().debug('get_all_security_current_day_tick_data_no_multi_thread Finish...')


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
    sh_thread_pool = stp.StlThreadPool(THREAD_COUNT)
    for code in code_list:
        slog.StlDmLogger().debug('get_current_day_tick_data, code: %s' % code)
        req = stp.StlWorkRequest(get_current_day_tick_data, args=[code], callback=print_result)
        sh_thread_pool.putRequest(req)
        slog.StlDmLogger().debug('work request #%s added to sh_thread_pool' % req.requestID)

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


def get_current_day_tick_data(code):
    '''
    获取code对应股票最新一个交易日的分笔交易信息

    Parameters
    ------
        code: 股票代码
    return
    -------
        无
    '''
    tick_dir_path = DEFAULT_DIR_PATH
    if not os.path.exists(tick_dir_path):
        os.makedirs(tick_dir_path)

    file_path = '%s/%s.xlsx' % (tick_dir_path, code)
    if not os.path.exists(file_path):
        try:
            tmp_data = pd.DataFrame()
            slog.StlDmLogger().debug('tushare.get_today_ticks: %s' % code)
            tmp_data = tushare.get_today_ticks(code, retry_count=RETRY_COUNT, pause=RETRY_PAUSE)
        except Exception as exception:
            slog.StlDmLogger().error('tushare.get_today_ticks(%s) excpetion, args: %s' % (code, exception.args.__str__()))

        if tmp_data is None:
            slog.StlDmLogger().warning('tushare.get_today_ticks(%s) return none' % code)
        else:
            tmp_data.to_excel(file_path)
    else:
        slog.StlDmLogger().debug('%s already exists' % file_path)


if __name__ == "__main__":
    # get_all_security_current_day_tick_data_multi_thread()
    get_all_security_current_day_tick_data_no_multi_thread()
    # get_current_day_tick_data('002612')


