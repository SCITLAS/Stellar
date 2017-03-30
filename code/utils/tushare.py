# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import os
import datetime


'''
TuShare工具方法
'''


def get_tushare_data_validate_date():
    '''
    获取tushare每日刷新数据的时间

    Parameters
    ------
        无
    return
    -------
        date: tushare数据刷新时间
    '''
    now = datetime.datetime.now()
    date_str = '%d-%02d-%02d %02d:%02d:%02d' % (now.year, now.month, now.day, 15, 0, 0)
    date = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    return date


def need_data_file_refresh(file_path):
    '''
    判断数据文件是否需要刷新

    Parameters
    ------
        file_path: 数据文件路径
    return
    -------
        bool: 文件不存在或者文件存在但创建时间早于数据有效时间:True,
              文件存在且创建日期晚于数据有效时间:False
    '''
    if os.path.exists(file_path):
        tt = os.path.getctime(file_path)
        file_date = datetime.datetime.fromtimestamp(tt)
        if file_date > get_tushare_data_validate_date():
            return False
    return True


