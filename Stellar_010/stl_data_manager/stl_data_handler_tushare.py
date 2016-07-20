# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


'''
获取证券股票的历史行情信息
存入对应的csv文件
'''


import tushare
from stl_utilities import stl_logger


def get_all_security_codes():
    '''
    获取所有证券股票代码

    Parameters
    ------
        无
    return
    -------
        list 所有证券股票代码
    '''



def get_security_all_history(code_list):
    '''
    获取code_list中所有证券股票历史行情信息,并将结果保存到对应csv文件

    tushare.get_h_data()可以查询指定股票所有的历史行情,
    数据只有7列:  date, open, hight, close, low, volume, amount

    tushare.get_hist_data()只能查询指定股票3年的历史行情,
    数据有14列: date, open, hight, close, low, volume, price_change, p_change, ma5, ma10, ma20, v_ma5,  v_ma10, v_ma20, turnover

    Parameters
    ------
        无
    return
    -------
        无
    '''

    for code in code_list:
        try:
            tmp_data_h = tushare.get_h_data(code, start='2000-01-01', end='2016-08-01')
        except Exception as exception:
            stl_logger.data_manager_logger(__file__).error('tushare.get_h_data(%s) excpetion, args: %s' % (code, exception.args.__str__()))

        if tmp_data_h is None:
            stl_logger.data_manager_logger(__file__).warning('tushare.get_h_data(%s) return none' % code)
        else:
            data_str_h = tmp_data_h.to_csv()
            with open('../data/origin/tushare/sh/%s.csv' % code, 'wt') as fout:
                fout.write(data_str_h)


def get_security_recent_history(code_list):
    '''
    获取code_list中所有证券股票近3年的历史行情信息,并将结果保存到对应csv文件

    tushare.get_hist_data()只能查询指定股票3年的历史行情,
    数据有14列: date, open, hight, close, low, volume, price_change, p_change, ma5, ma10, ma20, v_ma5,  v_ma10, v_ma20, turnover

    Parameters
    ------
        无
    return
    -------
        无
    '''

    for code in code_list:
        try:
            tmp_data_hist = tushare.get_hist_data(code, start='2000-01-01', end='2016-08-01')
        except Exception as exception:
            stl_logger.data_manager_logger(__file__).error('tushare.get_hist_data(%s) excpetion, args: %s' % (code, exception.args.__str__()))

        if tmp_data_hist is None:
            stl_logger.data_manager_logger(__file__).warning('tushare.get_hist_data(%s) return none' % code)
        else:
            data_str_hist = tmp_data_hist.to_csv()
            with open('../data/origin/tushare/sh/%s-r3.csv' % code, 'wt') as fout2:
                fout2.write(data_str_hist)


def get_all_security_history():
    '''
    获取所有证券股票近3年的历史行情信息,并将结果保存到对应csv文件

    tushare.get_hist_data()只能查询指定股票3年的历史行情,
    数据有14列: date, open, hight, close, low, volume, price_change, p_change, ma5, ma10, ma20, v_ma5,  v_ma10, v_ma20, turnover

    Parameters
    ------
        无
    return
    -------
        无
    '''


def test():
    get_security_all_history(['600004'])
    get_security_recent_history(['600004'])


if __name__ == "__main__":
    test()
