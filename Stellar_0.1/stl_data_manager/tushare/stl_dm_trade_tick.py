# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


from stl_utilities import stl_logger as slog
from stl_utilities import stl_thread_pool as stp
from stl_data_manager.tushare import stl_dm_fundamental as sfund

import os
import datetime
import pandas as pd
import tushare


'''
获取证券股票的指定交易日的分笔(TICK)行情信息
存入对应的csv文件
'''


thread_count = 50    # 查询交易数据的并行线程数
retry_count = 5      # 调用tushare接口失败重试次数
retry_pause = 0.1    # 调用tushare接口失败重试间隔时间

tick_forward = 0
tick_backward = 1


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
        if direction == tick_backward:
            tick_step = -offset
        else:
            tick_step = offset
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
    pass


def get_tick_data(code, tick_date):
    '''
    获取所有股票在指定时间长度内的分笔交易信息

    Parameters
    ------
        code: 股票代码
        tick_date: 查询日期
    return
    -------
        无
    '''
    tick_dir_path = '../../data/origin/tushare/security_trade_data/tick/%s' % tick_date
    if not os.path.exists(tick_dir_path):
        os.makedirs(tick_dir_path)

    file_path = '%s/%s.csv' % (tick_dir_path, code)
    if not os.path.exists(file_path):
        try:
            tmp_data = pd.DataFrame()
            slog.StlDmLogger().debug('tushare.get_tick_data: %s, tick_date=%s' % (code, tick_date))
            tmp_data = tushare.get_tick_data(code, tick_date, retry_count=retry_count, pause=retry_pause)
        except Exception as exception:
            slog.StlDmLogger().error('tushare.get_tick_data(%s) excpetion, args: %s' % (code, exception.args.__str__()))

        if tmp_data is None:
            slog.StlDmLogger().warning('tushare.get_tick_data(%s) return none' % code)
        else:
            data_str = tmp_data.to_csv()
            with open(file_path, 'w') as fout:
                fout.write(data_str)
    else:
        slog.StlDmLogger().debug('%s already exists' % file_path)




if __name__ == "__main__":
    # get_all_security_tick_data_multi_thread(start_date_str='2016-07-26', during=10, direction=tick_backward)
    get_all_security_tick_data_no_multi_thread(start_date_str='2016-07-26', during=10, direction=tick_backward)
    # get_tick_data('002612', '2016-07-26')