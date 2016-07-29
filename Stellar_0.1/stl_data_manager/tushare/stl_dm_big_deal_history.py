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
获取证券股票的指定交易日的大单交易数据
存入对应的csv文件
'''


THREAD_COUNT = 50    # 查询交易数据的并行线程数
RETRY_COUNT = 5      # 调用tushare接口失败重试次数
RETRY_PAUSE = 0.1    # 调用tushare接口失败重试间隔时间
VOL = 400            # 定义为大单的手数

DEAL_FORWARD = 0
DEAL_BACKWARD = 1

DEFAULT_DIR_PATH = '../../../Data/origin/tushare/security_trade_data/big_deal/history'


def get_all_security_big_deal_no_multi_thread(start_date_str, during, direction, vol=VOL):
    '''
    获取所有股票在指定时间长度内的大单交易信息, 非多线程版本

    Parameters
    ------
        start_date: 查询开始日期
        during: 自start_date开始持续天数
        direction: 向start_date前还是向后查, 0:向前, 1:向后
        vol: 大单手数
    return
    -------
        无
    '''
    slog.StlDmLogger().debug('get_all_security_big_deal_no_multi_thread Begin...')

    code_list = sfund.get_all_security_basic_info()              # 获取所有股票的基本信息
    deal_date_str = start_date_str
    for offset in range(1, during):
        for code in code_list:
            slog.StlDmLogger().debug('get_big_deal_data, code: %s, deal_date: %s' % (code, deal_date_str))
            get_big_deal_data(code, deal_date_str, vol)
        if direction == DEAL_BACKWARD:
            tick_step = -offset
        else:
            tick_step = offset
        star_date = datetime.datetime.strptime(deal_date_str, '%Y-%m-%d')
        next_day = star_date + datetime.timedelta(days=tick_step)
        deal_date_str = datetime.datetime.strftime(next_day, '%Y-%m-%d')

    slog.StlDmLogger().debug('get_all_security_big_deal_no_multi_thread Finish...')


def get_all_security_big_deal_multi_thread(start_date_str, during, direction, vol=VOL):
    '''
    获取所有股票在指定时间长度内的大单交易数据, 多线程版本

    Parameters
    ------
        start_date: 查询开始日期
        during: 自start_date开始持续天数
        direction: 向start_date前还是向后查, 0:向前, 1:向后
        vol: 大单手数
    return
    -------
        无
    '''
    code_list = sfund.get_all_security_basic_info()              # 获取所有股票的基本信息
    deal_date_str = start_date_str

    sh_thread_pool = stp.StlThreadPool(THREAD_COUNT)
    for offset in range(1, during):
        for code in code_list:
            slog.StlDmLogger().debug('get_big_deal_data, code: %s, deal_date: %s' % (code, deal_date_str))
            req = stp.StlWorkRequest(get_big_deal_data, args=[code, deal_date_str, vol], callback=print_result)
            sh_thread_pool.putRequest(req)
            slog.StlDmLogger().debug('work request #%s added to sh_thread_pool' % req.requestID)
        if direction == DEAL_BACKWARD:
            tick_step = -offset
        else:
            tick_step = offset
        star_date = datetime.datetime.strptime(deal_date_str, '%Y-%m-%d')
        next_day = star_date + datetime.timedelta(days=tick_step)
        deal_date_str = datetime.datetime.strftime(next_day, '%Y-%m-%d')

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


def get_big_deal_data(code, deal_date, vol=VOL):
    '''
    获取code对应股票在指定时间长度内的大单交易数据

    Parameters
    ------
        code: 股票代码
        tick_date: 查询日期
        vol: 大单手数
    return
    -------
        无
    '''
    deal_dir_path = '%s/%s(%d)' % (DEFAULT_DIR_PATH, deal_date, vol)
    if not os.path.exists(deal_dir_path):
        os.makedirs(deal_dir_path)

    file_path = '%s/%s.csv' % (deal_dir_path, code)
    if not os.path.exists(file_path):
        try:
            tmp_data = pd.DataFrame()
            slog.StlDmLogger().debug('tushare.get_sina_dd: %s, tick_date=%s' % (code, deal_date))
            tmp_data = tushare.get_sina_dd(code, date=deal_date, vol=vol, retry_count=RETRY_COUNT, pause=RETRY_PAUSE)
        except Exception as exception:
            slog.StlDmLogger().error('tushare.get_sina_dd(%s) excpetion, args: %s' % (code, exception.args.__str__()))

        if tmp_data is None:
            slog.StlDmLogger().warning('tushare.get_sina_dd(%s) return none' % code)
        else:
            data_str = tmp_data.to_csv()
            with open(file_path, 'w') as fout:
                fout.write(data_str)
    else:
        slog.StlDmLogger().debug('%s already exists' % file_path)


if __name__ == "__main__":
    # get_all_security_big_deal_no_multi_thread(start_date_str='2016-07-26', during=10, direction=DEAL_BACKWARD, vol=120)
    # get_all_security_big_deal_multi_thread(start_date_str='2016-07-26', during=10, direction=DEAL_BACKWARD, vol=200)
    get_big_deal_data('002612', '2016-07-26', vol=150)


