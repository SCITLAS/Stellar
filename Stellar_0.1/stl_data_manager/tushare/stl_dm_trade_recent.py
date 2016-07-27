# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


from stl_utilities import stl_logger as slog
from stl_utilities import stl_thread_pool as stp
from stl_utilities import stl_file_utilities as sfu
from stl_data_manager.tushare import stl_dm_fundamental as sfund

import os
import datetime
import time
import linecache
import pandas as pd
import tushare


'''
获取证券股票的近期行情信息
存入对应的csv文件
'''


thread_count = 50    # 查询交易数据的并行线程数
retry_count = 5      # 调用tushare接口失败重试次数
retry_pause = 0.1    # 调用tushare接口失败重试间隔时间
autype ='qfq'        # 复权类型，qfq-前复权 hfq-后复权 None-不复权
drop_factor = True   # 是否移除复权因子，在分析过程中可能复权因子意义不大，但是如需要先储存到数据库之后再分析的话，有该项目会更加灵活


def get_all_index_recent_data():
    '''
    获取A股所有指数近3年的信息

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

    file_path = ''
    if type == 'D':
        file_path = '../../data/origin/tushare/index_trade_data/recent/day/%s.csv' % file_name
    elif type == 'W':
        file_path = '../../data/origin/tushare/index_trade_data/recent/week/%s.csv' % file_name
    elif type == 'M':
        file_path = '../../data/origin/tushare/index_trade_data/recent/month/%s.csv' % file_name
    elif type == '5':
        file_path = '../../data/origin/tushare/index_trade_data/recent/5min/%s.csv' % file_name
    elif type == '15':
        file_path = '../../data/origin/tushare/index_trade_data/recent/15min/%s.csv' % file_name
    elif type == '30':
        file_path = '../../data/origin/tushare/index_trade_data/recent/30min/%s.csv' % file_name
    elif type == '60':
        file_path = '../../data/origin/tushare/index_trade_data/recent/60min/%s.csv' % file_name

    (is_update, start_date_str, end_date_str) = get_input_para(file_path)
    if start_date_str == end_date_str:
        slog.StlDmLogger().debug('%s data is already up-to-date.' % file_path)
    else:
        tmp_data_hist = pd.DataFrame()
        try:
            if start_date_str == '2000-01-01':
                start_date_str = start_date
            slog.StlDmLogger().debug('tushare.get_h_data: %s, start=%s, end=%s' % (code, start_date_str, end_date_str))
            tmp_data_hist = tushare.get_hist_data(code, start=start_date_str, end=end_date_str, ktype=type, retry_count=retry_count, pause=retry_pause)
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


def get_all_security_recent_data_multi_thread():
    '''
    获取所有股票近期行情, 并存入到对应的csv文件中, 多线程版本

    Parameters
    ------
        无
    return
    -------
        无
    '''
    slog.StlDmLogger().debug('get_all_security_recent_data_multi_thread (%d threads) Begin...' % thread_count)

    get_all_index_recent_data()                                  # 获取指数信息
    code_list = sfund.get_all_security_basic_info()              # 获取所有股票的基本信息
    get_recent_data_in_code_list_multi_thread(code_list, 'D', thread_count)   # 获取最近3年所有股票的日线数据
    get_recent_data_in_code_list_multi_thread(code_list, 'W', thread_count)   # 获取最近3年所有股票的周线数据
    get_recent_data_in_code_list_multi_thread(code_list, 'M', thread_count)   # 获取最近3年所有股票的月线数据
    get_recent_data_in_code_list_multi_thread(code_list, '5', thread_count)   # 获取最近3年所有股票的5分钟线数据
    get_recent_data_in_code_list_multi_thread(code_list, '15', thread_count)  # 获取最近3年所有股票的15分钟线数据
    get_recent_data_in_code_list_multi_thread(code_list, '30', thread_count)  # 获取最近3年所有股票的30分钟线数据
    get_recent_data_in_code_list_multi_thread(code_list, '60', thread_count)  # 获取最近3年所有股票的60分钟线数据

    slog.StlDmLogger().debug('get_all_security_recent_data_multi_thread (%d threads)Finish...' % thread_count)


def get_recent_data_in_code_list_multi_thread(code_list, type, thread_count):
    '''
    获取code对应股票的近3年历史行情信息,并将结果保存到对应csv文件, 多线程版本

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
        req = stp.StlWorkRequest(get_recent_data_of_code, args=[code, type], callback=print_result)
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


def get_recent_data_of_code(code, type):
    '''
    获取code对应股票的近3年历史行情信息,并将结果保存到对应csv文件

    tushare.get_hist_data()查询指定股票3年的历史行情, 返回数据如下:
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
        code: 股票代码
        type: 数据类型：D=日k线 W=周 M=月 5=5分钟 15=15分钟 30=30分钟 60=60分钟
    return
    -------
        无
    '''
    if type == 'D':
        file_path = '../../data/origin/tushare/security_trade_data/recent/day/%s.csv' % code
    elif type == 'W':
        file_path = '../../data/origin/tushare/security_trade_data/recent/week/%s.csv' % code
    elif type == 'M':
        file_path = '../../data/origin/tushare/security_trade_data/recent/month/%s.csv' % code
    elif type == '5':
        file_path = '../../data/origin/tushare/security_trade_data/recent/5min/%s.csv' % code
    elif type == '15':
        file_path = '../../data/origin/tushare/security_trade_data/recent/15min/%s.csv' % code
    elif type == '30':
        file_path = '../../data/origin/tushare/security_trade_data/recent/30min/%s.csv' % code
    elif type == '60':
        file_path = '../../data/origin/tushare/security_trade_data/recent/60min/%s.csv' % code

    (is_update, start_date_str, end_date_str) = get_input_para(file_path)
    if start_date_str == end_date_str:
        slog.StlDmLogger().debug('%s data is already up-to-date.' % file_path)
    else:
        tmp_data_hist = pd.DataFrame()
        try:
            slog.StlDmLogger().debug('tushare.get_hist_data: %s, start=%s, end=%s' % (code, start_date_str, end_date_str))
            tmp_data_hist = tushare.get_hist_data(code, start=start_date_str, end=end_date_str, ktype=type, retry_count=retry_count, pause=retry_pause)
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


def print_result(request, result):
    print("---Result from request %s : %r" % (request.requestID, result))


def check_data_integrity(data_path):
    '''
    对比data_path对应的文件夹中所有csv文件和basic_info.csv中code是否对应

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


def get_all_security_recent_data_no_multi_thread():
    '''
    获取所有股票历史行情, 并存入到对应的csv文件中, 不使用多线程

    Parameters
    ------
        无
    return
    -------
        无
    '''
    slog.StlDmLogger().debug('get_all_security_history_no_multi_thread Begin...')

    get_all_index_recent_data()

    code_list = sfund.get_all_security_basic_info()

    for code in code_list:
        get_recent_data_of_code(code, 'D')    # 获取最近3年所有股票的日线数据
        get_recent_data_of_code(code, 'W')    # 获取最近3年所有股票的周线数据
        get_recent_data_of_code(code, 'M')    # 获取最近3年所有股票的月线数据
        get_recent_data_of_code(code, '5')    # 获取最近3年所有股票的5分钟线数据
        get_recent_data_of_code(code, '15')   # 获取最近3年所有股票的15分钟线数据
        get_recent_data_of_code(code, '30')   # 获取最近3年所有股票的30分钟线数据
        get_recent_data_of_code(code, '60')   # 获取最近3年所有股票的60分钟线数据

    slog.StlDmLogger().debug('get_all_security_history_no_multi_thread Finish...')


if __name__ == "__main__":
    # get_all_security_recent_data_multi_thread()
    # get_all_security_recent_data_no_multi_thread()
    get_all_index_recent_data()






