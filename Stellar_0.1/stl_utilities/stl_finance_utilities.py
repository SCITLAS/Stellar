# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


'''
金融方面的工具方法
'''


import tushare


def get_all_code():
    '''
    获取所有A股所有股票的代码

    调用tushare.get_stock_basics()方法可以获得所有股票的基本信息

    Parameters
    ------
        无
    return
    -------
        list A股所有股票代码
    '''

    file_path = '../data/origin/tushare/security_fundamental_info/basic_info.csv'

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


def get_all_code_sh():
    '''
    获取所有上海交易所A股代码

    600xxx
    601xxx
    602xxx
    603xxx

    Parameters
    ------
        无
    return
    -------
        list 上海交易所A股代码
    '''

    result_list = []
    for code_int in range(600000, 603999):
        code_str = "%06d" % code_int
        result_list.append(code_str)
    return result_list

def get_all_code_sz():
    '''
    获取所有深圳交易所A股代码

    000xxx
    001xxx

    Parameters
    ------
        无
    return
    -------
        list 深圳交易所A股代码
    '''

    result_list = []
    for code_int in range(1, 1999):
        code_str = "%06d" % code_int
        result_list.append(code_str)
    return result_list

def get_all_code_sme():
    '''
    获取所有中小板A股代码

    002xxx

    Parameters
    ------
        无
    return
    -------
        list 中小板A股代码
    '''

    result_list = []
    for code_int in range(2000, 2999):
        code_str = "%06d" % code_int
        result_list.append(code_str)
    return result_list

def get_all_code_gem():
    '''
    获取所有创业板A股代码

    300xxx

    Parameters
    ------
        无
    return
    -------
        list 创业板A股代码
    '''

    result_list = []
    for code_int in range(300000, 300999):
        code_str = "%06d" % code_int
        result_list.append(code_str)
    return result_list