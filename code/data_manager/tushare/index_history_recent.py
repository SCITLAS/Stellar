# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import datetime
import os
import pandas as pd
import linecache

import tushare

from code.utils.logger import dm_log
from code.utils.common import get_stellar_root


'''
获取指数的近期行情信息
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

THREAD_COUNT = 50    # 查询交易数据的并行线程数
RETRY_COUNT = 5      # 调用tushare接口失败重试次数
RETRY_PAUSE = 0.1    # 调用tushare接口失败重试间隔时间
DROP_FACTOR = True   # 是否移除复权因子，在分析过程中可能复权因子意义不大，但是如需要先储存到数据库之后再分析的话，有该项目会更加灵活

DEFAULT_DIR_PATH = ('%s/Data/origin/tushare/index_trade_data/recent' % get_stellar_root())


def get_directory_path():
    dir_path = ''
    if STORAGE_MODE == USING_CSV:
        dir_path = '%s/index_trade_data/recent' % DEFAULT_CSV_PATH_TS
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    return dir_path


def get_csv_path(code, type):
    dir_path = '.'
    file_name = ''
    if code == 'sh':
        file_name = '000001'
    elif code == 'sz':
        file_name = '399001'
    elif code == 'hs300':
        file_name = '000300'
    elif code == 'sz50':
        file_name = '000016'
    elif code == 'zxb':
        file_name = '399005'
    elif code == 'cyb':
        file_name = '399006'

    if type == 'D':
        dir_path = '%s/day' % DEFAULT_DIR_PATH
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    elif type == 'W':
        dir_path = '%s/week' % DEFAULT_DIR_PATH
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    elif type == 'M':
        dir_path = '%s/month' % DEFAULT_DIR_PATH
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    elif type == '5':
        dir_path = '%s/5min' % DEFAULT_DIR_PATH
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    elif type == '15':
        dir_path = '%s/15min' % DEFAULT_DIR_PATH
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    elif type == '30':
        dir_path = '%s/30min' % DEFAULT_DIR_PATH
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    elif type == '60':
        dir_path = '%s/60min' % DEFAULT_DIR_PATH
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

    file_path = '%s/%s.csv' % (dir_path, file_name)
    return file_path


def get_all_index_recent_data():
    '''
    获取所有指数近3年的信息

    调用tushare.get_hist_data()方法获得所有指数信息，并存入对应的csv文件中

    Parameters
    ------
        无
    return
    -------
        无
    '''
    # 查询的开始时间有限制,如果开始时间一个月没有数据,就查不到数据,所以需要设置一下,
    # 下面这些开始时间,都是我人肉实验得出的数据.
    sh_start_date = '2000-01-01'
    sz_start_date = '2000-01-01'
    hs300_start_date = '2005-01-01'
    sz50_start_date = '2004-01-01'
    sme_start_date_1 = '2005-06-01'
    gem_start_date_1 = '2010-08-01'

    #近期数据
    ## sh=上证指数 sz=深圳成指 hs300=沪深300指数 sz50=上证50 zxb=中小板 cyb=创业板
    get_index_recent_data('sh', start_date=sh_start_date, type='5')          # 上证指数
    get_index_recent_data('sh', start_date=sh_start_date, type='15')         # 上证指数
    get_index_recent_data('sh', start_date=sh_start_date, type='30')         # 上证指数
    get_index_recent_data('sh', start_date=sh_start_date, type='60')         # 上证指数
    get_index_recent_data('sh', start_date=sh_start_date, type='D')          # 上证指数
    get_index_recent_data('sh', start_date=sh_start_date, type='W')          # 上证指数
    get_index_recent_data('sh', start_date=sh_start_date, type='M')          # 上证指数

    get_index_recent_data('sz', start_date=sz_start_date, type='5')          # 深圳成指
    get_index_recent_data('sz', start_date=sz_start_date, type='15')         # 深圳成指
    get_index_recent_data('sz', start_date=sz_start_date, type='30')         # 深圳成指
    get_index_recent_data('sz', start_date=sz_start_date, type='60')         # 深圳成指
    get_index_recent_data('sz', start_date=sz_start_date, type='D')          # 深圳成指
    get_index_recent_data('sz', start_date=sz_start_date, type='W')          # 深圳成指
    get_index_recent_data('sz', start_date=sz_start_date, type='M')          # 深圳成指

    get_index_recent_data('hs300', start_date=hs300_start_date, type='5')    # 沪深300
    get_index_recent_data('hs300', start_date=hs300_start_date, type='15')   # 沪深300
    get_index_recent_data('hs300', start_date=hs300_start_date, type='30')   # 沪深300
    get_index_recent_data('hs300', start_date=hs300_start_date, type='60')   # 沪深300
    get_index_recent_data('hs300', start_date=hs300_start_date, type='D')    # 沪深300
    get_index_recent_data('hs300', start_date=hs300_start_date, type='W')    # 沪深300
    get_index_recent_data('hs300', start_date=hs300_start_date, type='M')    # 沪深300

    get_index_recent_data('sz50', start_date=sz50_start_date, type='5')      # 上证50
    get_index_recent_data('sz50', start_date=sz50_start_date, type='15')     # 上证50
    get_index_recent_data('sz50', start_date=sz50_start_date, type='30')     # 上证50
    get_index_recent_data('sz50', start_date=sz50_start_date, type='60')     # 上证50
    get_index_recent_data('sz50', start_date=sz50_start_date, type='D')      # 上证50
    get_index_recent_data('sz50', start_date=sz50_start_date, type='W')      # 上证50
    get_index_recent_data('sz50', start_date=sz50_start_date, type='M')      # 上证50

    get_index_recent_data('zxb', start_date=sme_start_date_1, type='5')      # 中小板指数
    get_index_recent_data('zxb', start_date=sme_start_date_1, type='15')     # 中小板指数
    get_index_recent_data('zxb', start_date=sme_start_date_1, type='30')     # 中小板指数
    get_index_recent_data('zxb', start_date=sme_start_date_1, type='60')     # 中小板指数
    get_index_recent_data('zxb', start_date=sme_start_date_1, type='D')      # 中小板指数
    get_index_recent_data('zxb', start_date=sme_start_date_1, type='W')      # 中小板指数
    get_index_recent_data('zxb', start_date=sme_start_date_1, type='M')      # 中小板指数

    get_index_recent_data('cyb', start_date=gem_start_date_1, type='5')      # 中小板指数
    get_index_recent_data('cyb', start_date=gem_start_date_1, type='15')     # 中小板指数
    get_index_recent_data('cyb', start_date=gem_start_date_1, type='30')     # 中小板指数
    get_index_recent_data('cyb', start_date=gem_start_date_1, type='60')     # 中小板指数
    get_index_recent_data('cyb', start_date=gem_start_date_1, type='D')      # 中小板指数
    get_index_recent_data('cyb', start_date=gem_start_date_1, type='W')      # 中小板指数
    get_index_recent_data('cyb', start_date=gem_start_date_1, type='M')      # 中小板指数


def get_index_recent_data(code, start_date, type):
    '''
    获取code对应指数的近期行情信息,并将结果保存到对应csv文件

    tushare.get_hist_data()查询指定股票3年的历史行情,
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
        code: 股票代码, sh=上证指数 sz=深圳成指 hs300=沪深300指数 sz50=上证50 zxb=中小板 cyb=创业板
        start_date: 查询开始日期
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
                if start_date_str == '2000-01-01':
                    start_date_str = start_date
                dm_log.debug('tushare.get_h_data: %s, start=%s, end=%s called' % (code, start_date_str, end_date_str))
                df = tushare.get_hist_data(code, start=start_date_str, end=end_date_str, ktype=type, retry_count=RETRY_COUNT, pause=RETRY_PAUSE)
            except Exception as exception:
                dm_log.error('tushare.get_hist_data(%s) excpetion, args: %s' % (code, exception.args.__str__()))
            else:
                if df is None:
                    dm_log.warning('tushare.get_hist_data(%s) return none' % code)
                else:
                    dm_log.debug('tushare.get_h_data: %s, start=%s, end=%s done, got %d rows' % (code, start_date_str, end_date_str, len(df)))
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


if __name__ == "__main__":
    get_all_index_recent_data()






