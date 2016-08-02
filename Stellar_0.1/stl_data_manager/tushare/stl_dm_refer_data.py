__author__ = 'MoroJoJo'
# coding = utf-8


from stl_utils import stl_logger as slog

import tushare
import pandas as pd
import os


'''
投资参考数据
'''


PROFIT_INFO_COUNT = 200       # 获取的分配预案数据条数
DATA_YEAR = 2016              # 获取数据的年份
DATA_QUARTER = 1              # 获取数据的年份

RETRY_COUNT = 5               # 调用tushare接口失败重试次数
RETRY_PAUSE = 0.1             # 调用tushare接口失败重试间隔时间

DEFAULT_DIR_PATH = '../../../Data/origin/tushare/security_reference_data/'


def get_profit_data(year, top):
    '''
    获取指定年份的股票的分配预案

    Parameters
    ------
        year: 预案公布的年份
        top: 取最新数据的条数
    return
    -------
        无
    '''
    dir_path = DEFAULT_DIR_PATH
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = '%s/profit.csv' % dir_path

    tmp_data = pd.DataFrame()
    try:
        slog.StlDmLogger().debug('tushare.profit_data: %s, top=%d' % (year, PROFIT_INFO_COUNT))
        tmp_data = tushare.profit_data(year=year, top=top, retry_count=RETRY_COUNT, pause=RETRY_PAUSE)
    except Exception as exception:
        slog.StlDmLogger().error('tushare.profit_data(%s) excpetion, args: %s' % (year, exception.args.__str__()))

    if tmp_data is None:
        slog.StlDmLogger().warning('tushare.profit_data(%s) return none' % year)
    else:
        data_str_hist = tmp_data.to_csv()
        with open(file_path, 'w') as fout:
            fout.write(data_str_hist)



def get_forcast_data(year, quarter):
    '''
    获取指定年份季度的股票的业绩预告

    Parameters
    ------
        year: 年度
        quarter: 季度
    return
    -------
        无
    '''
    dir_path = DEFAULT_DIR_PATH
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = '%s/forecast.csv' % dir_path

    tmp_data = pd.DataFrame()
    try:
        slog.StlDmLogger().debug('tushare.forecast_data: %d, top=%d' % (year, quarter))
        tmp_data = tushare.forecast_data(year=year, quarter= quarter)
    except Exception as exception:
        slog.StlDmLogger().error('tushare.forecast_data(%d, %d) excpetion, args: %s' % (year, quarter, exception.args.__str__()))

    if tmp_data is None:
        slog.StlDmLogger().warning('tushare.forecast_data(%d, %d) return none' % (year, quarter))
    else:
        data_str_hist = tmp_data.to_csv()
        with open(file_path, 'w') as fout:
            fout.write(data_str_hist)


if __name__ == '__main__':
    get_profit_data(DATA_YEAR, PROFIT_INFO_COUNT)
    get_forcast_data(DATA_YEAR, DATA_QUARTER)