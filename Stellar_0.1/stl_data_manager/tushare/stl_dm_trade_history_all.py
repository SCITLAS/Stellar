# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


from stl_utils import stl_logger as slog
from stl_utils import stl_thread_pool as stp
from stl_utils import stl_file_utils as sfu
from stl_data_manager.tushare import stl_dm_fundamental as sfund

import os
import datetime
import time
import linecache
import pandas as pd
import tushare


'''
获取证券股票的历史行情信息
存入对应的xlsx文件
'''

'''
已知问题:
1. tushare.get_h_data()接口无法获取到000033的行情信息, 返回为None, 已经反馈给tushare作者.
   他说是因为停牌, 这支股票2015年4月29日最后一次交易,然后停牌至今.
   照理说, 设置查询的额截止日期到2015-04-29,应该是可以查询到数据的, 如下:
   tushare.get_h_data('000033', start='2000-01-01', end='2015-04-29')
   但以上调用依然返回None, 实际上这个接口查不到000033任何时间段的行情数据.

'''


THREAD_COUNT = 50    # 查询交易数据的并行线程数
RETRY_COUNT = 5      # 调用tushare接口失败重试次数
RETRY_PAUSE = 0.1    # 调用tushare接口失败重试间隔时间
AUTYPE ='qfq'        # 复权类型，qfq-前复权 hfq-后复权 None-不复权
DROP_FACTOR = True   # 是否移除复权因子，在分析过程中可能复权因子意义不大，但是如需要先储存到数据库之后再分析的话，有该项目会更加灵活

DEFAULT_DIR_PATH = '../../../Data/origin/tushare/security_trade_data/trade/history/all'


def get_all_security_history_multi_thread():
    '''
    获取所有股票历史行情, 并存入到对应的xlsx文件中, 多线程版本

    Parameters
    ------
        无
    return
    -------
        无
    '''
    slog.StlDmLogger().debug('get_all_security_history_multi_thread (%d threads) Begin...')

    code_list = sfund.get_all_security_basic_info()                              # 获取所有股票的基本信息
    get_all_history_in_code_list_multi_thread(code_list, THREAD_COUNT)           # 获取自2000年1月1日以来的所有数据

    slog.StlDmLogger().debug('get_all_security_history_multi_thread (%d threads) Finish...')


def get_all_history_in_code_list_multi_thread(code_list, thread_count):
    '''
    获取code对应股票的所有历史行情信息,并将结果保存到对应xlsx文件, 多线程版本

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

    while True:
        try:
            time.sleep(0.5)
            sh_thread_pool.poll()
        except stp.StlNoResultsPendingException:
            slog.StlDmLogger().debug('No Pending Results')
            break
    sh_thread_pool.stop()


def get_all_history_of_code(code):
    '''
    获取code对应股票的所有历史行情信息, 并将结果保存到对应xlsx文件

    tushare.get_h_data()可以查询指定股票所有的历史行情, 返回数据如下:
        date : 交易日期 (index)
        open : 开盘价
        high : 最高价
        close : 收盘价
        low : 最低价
        volume : 成交量
        amount : 成交金额

    Parameters
    ------
        code: 股票代码
    return
    -------
        无
    '''
    dir_path = DEFAULT_DIR_PATH
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = '%s/%s.xlsx' % (dir_path, code)
    (is_update, start_date_str, end_date_str) = get_input_para(file_path)
    if start_date_str == end_date_str:
        slog.StlDmLogger().debug('%s data is already up-to-date.' % file_path)
    else:
        tmp_data_hist = pd.DataFrame()
        try:
            slog.StlDmLogger().debug('tushare.get_h_data: %s, start=%s, end=%s' % (code, start_date_str, end_date_str))
            tmp_data_hist = tushare.get_h_data(code, start=start_date_str, end=end_date_str, autype=AUTYPE, retry_count=RETRY_COUNT, pause=RETRY_PAUSE, drop_factor=DROP_FACTOR)
        except Exception as exception:
            slog.StlDmLogger().error('tushare.get_hist_data(%s) excpetion, args: %s' % (code, exception.args.__str__()))

        if tmp_data_hist is None:
            slog.StlDmLogger().warning('tushare.get_hist_data(%s) return none' % code)
        else:
            if is_update:
                old_data = pd.read_xlsx(file_path, index_col=0)
                all_data = tmp_data_hist.append(old_data)
                all_data.to_excel(file_path)
            else:
                tmp_data_hist.to_excel(file_path)


def get_input_para(file_path):
    '''
    获取指定xlsx文件的中最新一条记录的信息，返回需要获取信息的起始日期，以及是更新还是新建

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


def print_result(request, result):
    print("---Result from request %s : %r" % (request.requestID, result))


def check_data_integrity(data_path):
    '''
    对比data_path对应的文件夹中所有xlsx文件和basic_info.xlsx中code是否对应

    Parameters
    ------
        data_path: 指定文件夹路径
    return
    -------
        missing_code_list: 缺失的code列表
    '''
    data_code_list = sfu.get_code_list_in_dir(data_path)
    missing_code_list = []
    basic_data = pd.DataFrame()
    try:
        basic_data = tushare.get_stock_basics()
    except Exception as exception:
        slog.StlDmLogger().error('tushare.get_stock_basics() excpetion, args: %s' % exception.args.__str__())
    if basic_data is None:
        slog.StlDmLogger().warning('tushare.get_stock_basics() return none')
        return []
    else:
        code_list = basic_data.index
        for code in code_list:
            found = False
            for tmp_code in data_code_list:
                if tmp_code == code:
                    found = True
                    break
            if found != True:
                missing_code_list.append(code)

    return missing_code_list


def get_all_security_history_no_multi_thread():
    '''
    获取所有股票历史行情, 并存入到对应的xlsx文件中, 不使用多线程

    Parameters
    ------
        无
    return
    -------
        无
    '''
    slog.StlDmLogger().debug('get_all_security_history_no_multi_thread Begin...')

    code_list = sfund.get_all_security_basic_info()
    for code in code_list:
        get_all_history_of_code(code)            # 获取自2000年1月1日以来的所有数据

    slog.StlDmLogger().debug('get_all_security_history_no_multi_thread Finish...')


if __name__ == "__main__":
    # get_all_security_history_multi_thread()
    get_all_security_history_no_multi_thread()






