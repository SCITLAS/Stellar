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
from stl_utilities import stl_thread_pool as stp


def get_all_security_basic_info():
    '''
    获取所有A股所有股票的基本信息

    调用tushare.get_stock_basics()方法获得所有股票的基本信息，并存入对应的csv文件中

    实测结果：
    CPU：Intel Core i5-4300U 1.9GHz
    内存：4G
    线程数：50

    完整获取所交易历史有数据，耗时287分钟，具体信息如下：
    获取7类近3年的数据数据耗时：20分钟
    获取2000年1月1日至今所有历史行情耗时：267分钟

    Parameters
    ------
        无
    return
    -------
        code_list: 股票代码列表
    '''

    file_path = '../data/origin/tushare/security_fundamental_data/basic_info.csv'
    try:
        basic_data = tushare.get_stock_basics()
    except Exception as exception:
        slog.StlDmLogger().error('tushare.get_stock_basics() excpetion, args: %s' % exception.args.__str__())
    if basic_data is None:
        slog.StlDmLogger().warning('tushare.get_stock_basics() return none')
        return []
    else:
        basic_data_str = basic_data.to_csv()
        with open(file_path, 'w') as fout:
            fout.write(basic_data_str)
        return basic_data.index.tolist()


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

    thread_count = 50
    slog.StlDmLogger().debug('Begin...')

    code_list = get_all_security_basic_info()

    get_recent_history_in_code_list(code_list, 'D', thread_count)   #获取最近3年所有股票的日线数据
    get_recent_history_in_code_list(code_list, 'W', thread_count)   #获取最近3年所有股票的周线数据
    get_recent_history_in_code_list(code_list, 'M', thread_count)   #获取最近3年所有股票的月线数据
    get_recent_history_in_code_list(code_list, '5', thread_count)   #获取最近3年所有股票的5分钟线数据
    get_recent_history_in_code_list(code_list, '15', thread_count)  #获取最近3年所有股票的15分钟线数据
    get_recent_history_in_code_list(code_list, '30', thread_count)  #获取最近3年所有股票的30分钟线数据
    get_recent_history_in_code_list(code_list, '60', thread_count)  #获取最近3年所有股票的60分钟线数据
    get_all_history_in_code_list(code_list, thread_count)           #获取最近3年所有股票自2000年1月1日以来的所有数据

    slog.StlDmLogger().debug('Finish...')


def get_all_history_in_code_list(code_list, thread_count):
    '''
    获取code对应股票的所有历史行情信息,并将结果保存到对应csv文件

    tushare.get_h_data()可以查询指定股票所有的历史行情,
    数据只有7列:  date, open, hight, close, low, volume, amount

    Parameters
    ------
        code_list: 股票代码列表
        thread_count:线程数
    return
    -------
        无
    '''

    sh_thread_pool = stp.StlThreadPool(thread_count)
    for code in code_list:
        req = stp.StlWorkRequest(get_all_history_of_code, args=[code], callback=print_result)
        sh_thread_pool.putRequest(req)
        slog.StlDmLogger().debug('work request #%s added to sh_thread_pool' % req.requestID)

    counter = 0
    while True:
        try:
            time.sleep(0.5)
            sh_thread_pool.poll()
            if counter == 5:
                slog.StlDmLogger().debug('Add 3 more worker threads')
                sh_thread_pool.createWorkers(3)
                slog.StlDmLogger().debug('-'*20, sh_thread_pool.workersize(), '-'*20)
            if counter == 10:
                slog.StlDmLogger().debug('dismiss 2 worker threads')
                sh_thread_pool.dismissedWorkers(2)
                slog.StlDmLogger().debug('-'*20, sh_thread_pool.workersize(), '-'*20)
        except stp.StlNoResultsPendingException:
            slog.StlDmLogger().debug('No Pending Results')
            break
    sh_thread_pool.stop()

def get_all_history_of_code(code):
    '''
    获取code对应股票的所有历史行情信息,并将结果保存到对应csv文件

    tushare.get_h_data()可以查询指定股票所有的历史行情,
    数据只有7列:  date, open, hight, close, low, volume, amount

    Parameters
    ------
        code: 股票代码
    return
    -------
        无
    '''

    file_path = '../data/origin/tushare/security_trade_data/all/%s.csv' % code
    (is_update, start_date_str, end_date_str) = get_input_para(file_path)
    if start_date_str == end_date_str:
        slog.StlDmLogger().debug('%s data is already up-to-date.' % file_path)
    else:
        try:
            tmp_data_hist = tushare.get_h_data(code, start=start_date_str, end=end_date_str)
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


def get_recent_history_in_code_list(code_list, type, thread_count):
    '''
    获取code对应股票的近3年历史行情信息,并将结果保存到对应csv文件

    tushare.get_hist_data()查询指定股票3年的历史行情,
    数据有14列: date, open, hight, close, low, volume, price_change, p_change, ma5, ma10, ma20, v_ma5,  v_ma10, v_ma20, turnover

    Parameters
    ------
        code_list: 股票代码列表
        type: 数据类型：D=日k线 W=周 M=月 5=5分钟 15=15分钟 30=30分钟 60=60分钟
        thread_count:线程数
    return
    -------
        无
    '''

    sh_thread_pool = stp.StlThreadPool(thread_count)
    for code in code_list:
        req = stp.StlWorkRequest(get_recent_history_of_code, args=[code, type], callback=print_result)
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

def get_recent_history_of_code(code, type):
    '''
    获取code对应股票的近3年历史行情信息,并将结果保存到对应csv文件

    tushare.get_hist_data()查询指定股票3年的历史行情,
    数据有14列: date, open, hight, close, low, volume, price_change, p_change, ma5, ma10, ma20, v_ma5,  v_ma10, v_ma20, turnover

    Parameters
    ------
        code: 股票代码
        type: 数据类型：D=日k线 W=周 M=月 5=5分钟 15=15分钟 30=30分钟 60=60分钟
    return
    -------
        无
    '''

    if type == 'D':
        file_path = '../data/origin/tushare/security_trade_data/recent/day/%s.csv' % code
    elif type == 'W':
        file_path = '../data/origin/tushare/security_trade_data/recent/week/%s.csv' % code
    elif type == 'M':
        file_path = '../data/origin/tushare/security_trade_data/recent/month/%s.csv' % code
    elif type == '5':
        file_path = '../data/origin/tushare/security_trade_data/recent/5min/%s.csv' % code
    elif type == '15':
        file_path = '../data/origin/tushare/security_trade_data/recent/15min/%s.csv' % code
    elif type == '30':
        file_path = '../data/origin/tushare/security_trade_data/recent/30min/%s.csv' % code
    elif type == '60':
        file_path = '../data/origin/tushare/security_trade_data/recent/60min/%s.csv' % code

    (is_update, start_date_str, end_date_str) = get_input_para(file_path)
    if start_date_str == end_date_str:
        slog.StlDmLogger().debug('%s data is already up-to-date.' % file_path)
    else:
        try:
            tmp_data_hist = tushare.get_hist_data(code, start=start_date_str, end=end_date_str, ktype=type)
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
            slog.StlDmLogger().debug('%s 2 line is none' % file_path)
        elif line == '':
            is_update = False
            slog.StlDmLogger().debug("%s 2 line is ''" % file_path)
        elif line == '\n':
            is_update = False
            slog.StlDmLogger().debug("%s 2 line is '\\n'" % file_path)
        else:
            is_update = True
            date_str = line[0:10]
            latest_date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
            today = datetime.datetime.today()
            if latest_date < today:
                next_day = latest_date + datetime.timedelta(days=1)
                start_date_str = datetime.datetime.strftime(next_day, '%Y-%m-%d')
    else:
        slog.StlDmLogger().debug('%s does not exist, do get all task' % file_path)

    return (is_update, start_date_str, end_date_str)

def print_result(request, result):
    print("---Result from request %s : %r" % (request.requestID, result))


if __name__ == "__main__":
    get_all_security_history()
