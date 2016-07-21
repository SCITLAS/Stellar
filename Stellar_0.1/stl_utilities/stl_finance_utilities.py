# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


'''
金融方面的工具方法
'''


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