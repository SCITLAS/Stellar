# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


'''
获取证券股票的历史行情信息
存入对应的csv文件
'''

import os
import datetime
import time
import linecache
import pandas as pd
import tushare
from stl_utilities import stl_logger as slog
from stl_utilities import stl_finance_utilities as sfu
from stl_utilities import stl_thread_pool


def get_all_security_history():
    '''
    获取所有股票历史行情, 并存入到对应的csv文件中

    Parameters
    ------
        无
    return
    -------
        无
    '''

    slog.data_manager_logger('Get Security History').debug('Begin...')

    #沪市股票
    for code1 in sfu.get_all_code_sh():
        get_security_history('sh', code1, True)
        get_security_history('sh', code1, False)

    #深市股票
    for code2 in sfu.get_all_code_sz():
        get_security_history('sz', code2, True)
        get_security_history('sz', code2, False)

    #中小板
    for code3 in sfu.get_all_code_sme():
        get_security_history('sme', code3, True)
        get_security_history('sme', code3, False)

    #创业板
    for code4 in sfu.get_all_code_gem():
        get_security_history('gem', code4, True)
        get_security_history('gem', code4, False)

    slog.data_manager_logger('Get Security History').debug('Finish...')


def get_security_history(type, code, is_all):
    '''
    获取code对应股票的所有历史行情信息,并将结果保存到对应csv文件

    tushare.get_h_data()可以查询指定股票所有的历史行情,
    数据只有7列:  date, open, hight, close, low, volume, amount

    tushare.get_hist_data()只能查询指定股票3年的历史行情,
    数据有14列: date, open, hight, close, low, volume, price_change, p_change, ma5, ma10, ma20, v_ma5,  v_ma10, v_ma20, turnover

    Parameters
    ------
        type: sh=上证股票, sz=深市股票, sme=中小板, gem=创业板
        code: 股票代码
        is_all: True:查询所有历史行情, False:查询近期(3年)历史行情
    return
    -------
        无
    '''

    if is_all:
        file_path = '../data/origin/tushare/%s/%s.csv' % (type, code)
    else:
        file_path = '../data/origin/tushare/%s/%s-r.csv' % (type, code)
    start_date_str = '2000-01-01'
    end_date_str = time.strftime('%Y-%m-%d')
    operator = 'w'
    date_str = ''

    if os.path.exists(file_path):
        print('%s exists, do update task' % file_path)
        operator = 'a'
        line = linecache.getline(file_path, 2)
        date_str = line[0:10]
        latest_date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        today = datetime.datetime.today()
        if latest_date < today:
            print('latest_date =', latest_date, ' , today =', today, ' later date is ', today)
            next_day = latest_date + datetime.timedelta(days=1)
            start_date_str = datetime.datetime.strftime(next_day, '%Y-%m-%d')
        else:
            print('latest_date =', latest_date, ' , today =', today, ' later date is ', latest_date)

    else:
        print('%s exists, do get all task' % file_path)

    print('type = %s, code = %s, lastest_date: %s, start_date: %s, end_date = %s' % (type, code, date_str, start_date_str, end_date_str))

    try:
        if is_all:
            tmp_data_hist = tushare.get_h_data(code, start=start_date_str, end=end_date_str)
        else:
            tmp_data_hist = tushare.get_hist_data(code, start=start_date_str, end=end_date_str)
    except Exception as exception:
        slog.dm_logger(__file__).error('tushare.get_hist_data(%s) excpetion, args: %s' % (code, exception.args.__str__()))

    if tmp_data_hist is None:
        slog.dm_logger(__file__).warning('tushare.get_hist_data(%s) return none' % code)
    else:
        if operator == 'a':
            old_data = pd.read_csv(file_path)
            tmp_data_hist.append(old_data)
            print(tmp_data_hist)
        data_str_hist = tmp_data_hist.to_csv()
        with open(file_path, operator) as fout:
            fout.write(data_str_hist)


if __name__ == "__main__":
    get_security_history('sh', '600004', False)

