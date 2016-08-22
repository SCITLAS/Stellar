# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import time
from apscheduler.schedulers.background import BackgroundScheduler

import tushare

from stl_utils.logger import dm_log


'''
获取所有指数的实时行情信息
'''


def get_index_real_time_data():
    '''
    获取所有指数的实时数据,并将结果保存到对应csv文件

    tushare.get_index()查询所有指数一个交易日行情, 返回数据如下:
        code:指数代码
        name:指数名称
        change:涨跌幅
        open:开盘点位
        preclose:昨日收盘点位
        close:收盘点位
        high:最高点位
        low:最低点位
        volume:成交量(手)
        amount:成交金额（亿元）

    Parameters
    ------
        无
    return
    -------
        data_dict 或者 None: 异常或没有查到数据返回None, 否则返回由dataframe.to_dict()方法传出的字典对象,
                            key为dataframe的列名, value为字典{index:列值}, 比如:
                            'name': {0: '上证指数',
                                     1: 'Ａ股指数',
                                     2: 'Ｂ股指数',
                                     3: '综合指数',
                                     4: '上证380',
                                     5: '上证180',
                                     6: '基金指数',
                                     7: '国债指数',
                                     8: '上证50',
                                      ......
                                     23: '创业板R'}
    '''
    try:
        df = tushare.get_index()
    except Exception as exception:
        dm_log.error('tushare.get_index() excpetion, args: %s' % exception.args.__str__())
    else:
        if df is None:
            dm_log.warning('tushare.get_index() return none')
            return None
        else:
            data_dict = df.to_dict()
            dm_log.debug('tushare.get_index() data: %s' % data_dict)
            return data_dict


def start_get_index_real_time_data():
    '''
    定时获取code对应股票实时分笔交易信息

    定时调用get_realtime_tick_data()

    Parameters
    ------
        无
    return
    -------
        scheduler: 定时器
    '''
    scheduler = BackgroundScheduler()
    try:
        scheduler.add_job(get_index_real_time_data, trigger='cron', second='*/3', hour='*')
        scheduler.start()
    except (Exception):
        scheduler.shutdown()
        return None
    else:
        return scheduler

if __name__ == "__main__":
    dm_log.debug('start get real time index data')
    scheduler = start_get_index_real_time_data()
    time.sleep(20)  # 经测试发现, 网速一般的环境下, get_index_real_time_data(), 20秒能查6到7次
    dm_log.debug('finish get real time index data')
    scheduler.shutdown()

