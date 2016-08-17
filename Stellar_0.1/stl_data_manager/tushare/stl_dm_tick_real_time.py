# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import time

import tushare

from stl_utils import stl_logger as slog
from stl_data_manager.tushare import stl_dm_fundamental as sfund
from apscheduler.schedulers.background import BackgroundScheduler


'''
获取证券股票的实时分笔(TICK)行情信息
'''


THREAD_COUNT = 50    # 查询交易数据的并行线程数
RETRY_COUNT = 5      # 调用tushare接口失败重试次数
RETRY_PAUSE = 0.1    # 调用tushare接口失败重试间隔时间

REAL_TIME_TICK_INTERVAL = 2


def get_all_security_real_time_tick_data_no_multi_thread():
    '''
    获取所有股票实时分笔交易信息, 非多线程版本

    Parameters
    ------
        无
    return
    -------
        无
    '''
    slog.StlDmLogger().debug('get_all_security_real_time_tick_data_no_multi_thread Begin...')

    code_list = sfund.get_all_security_basic_info()              # 获取所有股票的基本信息
    for code in code_list:
        slog.StlDmLogger().debug('get_realtime_tick_data, code: %s, tick_date: %s' % code)
        get_real_time_tick_data(code)

    slog.StlDmLogger().debug('get_all_security_real_time_tick_data_no_multi_thread Finish...')


def get_real_time_tick_data(code):
    '''
    获取code对应股票实时分笔交易信息

    调用 tushare.get_realtime_quotes() 接口,官方说明:
    获取实时分笔数据，可以实时取得股票当前报价和成交信息，其中一种场景是，写一个python定时程序来调用本接口（可两三秒执行一次，性能与行情软件基本一致），
    然后通过DataFrame的矩阵计算实现交易监控，可实时监测交易量和价格的变化.
    dataframe的列如下:

        0：name，股票名字
        1：open，今日开盘价
        2：pre_close，昨日收盘价
        3：price，当前价格
        4：high，今日最高价
        5：low，今日最低价
        6：bid，竞买价，即“买一”报价
        7：ask，竞卖价，即“卖一”报价
        8：volume，成交量 maybe you need do volume/100
        9：amount，成交金额（元 CNY）
        10：b1_v，委买一（笔数 bid volume）
        11：b1_p，委买一（价格 bid price）
        12：b2_v，“买二”
        13：b2_p，“买二”
        14：b3_v，“买三”
        15：b3_p，“买三”
        16：b4_v，“买四”
        17：b4_p，“买四”
        18：b5_v，“买五”
        19：b5_p，“买五”
        20：a1_v，委卖一（笔数 ask volume）
        21：a1_p，委卖一（价格 ask price）
        ...
        30：date，日期；
        31：time，时间；

    Parameters
    ------
        code: 股票代码
    return
    -------
        data_dict 或者 None: 异常或没有查到数据返回None, 否则返回由dataframe.to_dict()方法传出的字典对象,
                            key为dataframe的列名, value为字典{0:列值}, 比如: 'name': {0: '朗姿股份'}
    '''
    try:
        df = tushare.get_realtime_quotes(code)
    except Exception as exception:
        slog.StlDmLogger().error('tushare.get_real_time_tick_data(%s) excpetion, args: %s' % (code, exception.args.__str__()))
    else:
        if df is None:
            slog.StlDmLogger().warning('tushare.get_real_time_tick_data(%s) return none' % code)
            return []
        else:
            data_dict =df.to_dict()
            slog.StlDmLogger().debug('tushare.get_real_time_tick_data(%s) data: %s' % (code, data_dict))
            return data_dict


def start_get_real_time_tick(code):
    '''
    定时获取code对应股票实时分笔交易信息

    定时调用get_realtime_tick_data()

    Parameters
    ------
        code: 股票代码
    return
    -------
        scheduler: 定时器
        code:股票代码
    '''
    scheduler = BackgroundScheduler()
    try:
        scheduler.add_job(get_real_time_tick_data, args=[code], trigger='cron', second='*/3', hour='*')
        scheduler.start()
    except (Exception):
        scheduler.shutdown()
        return None
    else:
        return (scheduler, code)


if __name__ == "__main__":
    slog.StlDmLogger().debug('start get real time tick data of 002612')
    (scheduler, code) = start_get_real_time_tick('002612')
    time.sleep(20)  # 经测试发现, 网速一般的环境下, 3秒调一次get_realtime_tick_data(), 20秒能查6到7次
    slog.StlDmLogger().debug('finish get real time tick data of 002612')
    scheduler.shutdown()
