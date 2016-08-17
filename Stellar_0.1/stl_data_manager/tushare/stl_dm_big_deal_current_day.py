# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import os
import datetime
import time

import tushare

from stl_utils import stl_logger as slog
from stl_utils import stl_thread_pool as stp
from stl_data_manager.tushare import stl_dm_fundamental as sfund


'''
获取证券股票的指定交易日的大单交易数据
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
VOL = 400            # 定义为大单的手数

DEAL_FORWARD = 0
DEAL_BACKWARD = 1


def get_directory_path():
    dir_path = ''
    if STORAGE_MODE == USING_CSV:
        dir_path = '%s/security_trade_data/big_deal/current_day' % DEFAULT_CSV_PATH_TS
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    return dir_path


def get_all_security_current_day_big_deal_no_multi_thread(vol=VOL):
    '''
    获取所有股票在今天的大单交易信息, 非多线程版本

    Parameters
    ------
        vol: 大单手数
    return
    -------
        无
    '''
    slog.StlDmLogger().debug('get_all_security_current_day_big_deal_no_multi_thread Begin...')

    today = datetime.datetime.today()
    today_str = datetime.datetime.strftime(today, '%Y-%m-%d')
    code_list = sfund.get_all_security_basic_info()              # 获取所有股票的基本信息
    for code in code_list:
        slog.StlDmLogger().debug('get_big_deal_data, code: %s, deal_date: %s' % (code, today_str))
        get_big_deal_data(code, today_str, vol)

    slog.StlDmLogger().debug('get_all_security_current_day_big_deal_no_multi_thread Finish...')


def get_all_security_current_day_big_deal_multi_thread(vol=VOL):
    '''
    获取所有股票今天的大单交易数据, 多线程版本

    Parameters
    ------
        vol: 大单手数
    return
    -------
        无
    '''
    today = datetime.datetime.today()
    today_str = datetime.datetime.strftime(today, '%Y-%m-%d')
    code_list = sfund.get_all_security_basic_info()              # 获取所有股票的基本信息

    sh_thread_pool = stp.StlThreadPool(THREAD_COUNT)
    for code in code_list:
        slog.StlDmLogger().debug('get_big_deal_data, code: %s, deal_date: %s' % (code, today_str))
        req = stp.StlWorkRequest(get_big_deal_data, args=[code, today_str, vol], callback=print_result)
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


def get_big_deal_data(code, deal_date, vol=VOL):
    '''
    获取code对应股票在指定时间长度内的大单交易数据

        code：代码
        name：名称
        time：时间
        price：当前价格
        volume：成交手
        preprice ：上一笔价格
        type：买卖类型【买盘、卖盘、中性盘】

    Parameters
    ------
        code: 股票代码
        tick_date: 查询日期
        vol: 大单手数
    return
    -------
        无
    '''
    if STORAGE_MODE == USING_CSV:
        file_path = '%s/%s/vol_%s.csv' % (get_directory_path(), deal_date, vol)
        if not os.path.exists(file_path):
            try:
                slog.StlDmLogger().debug('tushare.get_sina_dd: %s, tick_date=%s' % (code, deal_date))
                df = tushare.get_sina_dd(code, date=deal_date, vol=vol, retry_count=RETRY_COUNT, pause=RETRY_PAUSE)
            except Exception as exception:
                slog.StlDmLogger().error('tushare.get_sina_dd(%s) excpetion, args: %s' % (code, exception.args.__str__()))
            else:
                if df is None:
                    slog.StlDmLogger().warning('tushare.get_sina_dd(%s) return none' % code)
                else:
                    df.to_csv(file_path)
        else:
            slog.StlDmLogger().debug('%s already exists' % file_path)


if __name__ == "__main__":
    # get_all_security_current_day_big_deal_no_multi_thread(vol=120)
    get_all_security_current_day_big_deal_multi_thread(vol=200)
    # get_big_deal_data('002612', '2016-07-26', vol=150)


