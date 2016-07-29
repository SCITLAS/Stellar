# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


from stl_utils import stl_logger as slog

import os
import datetime
import time
import linecache
import pandas as pd
import tushare


'''
获取指数的历史行情信息
存入对应的csv文件
'''


THREAD_COUNT = 50    # 查询交易数据的并行线程数
RETRY_COUNT = 5      # 调用tushare接口失败重试次数
RETRY_PAUSE = 0.1    # 调用tushare接口失败重试间隔时间
AUTYPE ='qfq'        # 复权类型，qfq-前复权 hfq-后复权 None-不复权
DROP_FACTOR = True   # 是否移除复权因子，在分析过程中可能复权因子意义不大，但是如需要先储存到数据库之后再分析的话，有该项目会更加灵活

DEFAULT_DIR_PATH = '../../../Data/origin/tushare/index_trade_data/history/all'


def get_all_index_data():
    '''
    获取A股所有指数近3年的信息, 并将结果保存到对应csv文件

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
    sme_start_date_2 = '2008-06-01'
    gem_start_date_1 = '2010-08-01'
    gem_start_date_2 = '2010-06-01'

    # 历史数据
    get_index_data('000001', sh_start_date)        # 上证指数
    get_index_data('399001', sz_start_date)        # 深圳成指
    get_index_data('000300', hs300_start_date)     # 沪深300
    get_index_data('000016', sz50_start_date)      # 上证50
    get_index_data('399101', sme_start_date_1)     # 中小板综合指数
    get_index_data('399005', sme_start_date_2)     # 中小板指数
    get_index_data('399102', gem_start_date_1)     # 创业板综合指数
    get_index_data('399006', gem_start_date_2)     # 创业板指数


def get_index_data(code, start_date):
    '''
    获取code对应指数的历史行情信息, 并将结果保存到对应csv文件

    Parameters
    ------
        code: 股票代码
        start_date: 查询开始日期
     return
    -------
        无
    '''
    dir_path = DEFAULT_DIR_PATH
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = '%s/%s.csv' % (dir_path, code)
    (is_update, start_date_str, end_date_str) = get_input_para(file_path)
    if start_date_str == end_date_str:
        slog.StlDmLogger().debug('%s data is already up-to-date.' % file_path)
    else:
        tmp_data_hist = pd.DataFrame()
        try:
            if start_date_str == '2000-01-01':
                start_date_str = start_date
            slog.StlDmLogger().debug('tushare.get_h_data: %s, start=%s, end=%s' % (code, start_date_str, end_date_str))
            tmp_data_hist = tushare.get_h_data(code, start=start_date_str, end=end_date_str, index=True, retry_count=RETRY_COUNT, pause=RETRY_PAUSE, drop_factor=DROP_FACTOR)
        except Exception as exception:
            slog.StlDmLogger().error('tushare.get_hist_data(%s) excpetion, args: %s' % (code, exception.args.__str__()))

        if tmp_data_hist is None:
            slog.StlDmLogger().warning('tushare.get_hist_data(%s) return none' % code)
        else:
            if is_update:
                old_data = pd.read_csv(file_path, index_col=0)
                all_data = tmp_data_hist.append(old_data)
                data_str_hist = all_data.to_csv()
            else:
                data_str_hist = tmp_data_hist.to_csv()
            with open(file_path, 'w') as fout:
                fout.write(data_str_hist)


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
    end_date_str = time.strftime('%Y-%m-%d')
    is_update = False
    if os.path.exists(file_path):
        slog.StlDmLogger().debug('%s exists, do update task' % file_path)
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
                start_date_str = datetime.datetime.strftime(next_day, '%Y-%m-%d')
            else:
                start_date_str = end_date_str
    else:
        slog.StlDmLogger().debug('%s does not exist, do get all task' % file_path)

    return (is_update, start_date_str, end_date_str)


if __name__ == "__main__":
    get_all_index_data()






