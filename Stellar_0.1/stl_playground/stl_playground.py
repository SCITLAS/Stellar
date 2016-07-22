# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


'''
轻量级的试验场地
'''


import pandas as pd
import tushare


def dataframe2csv_test():

    '''
    问题描述:
    dataframe存csv后, 再从csv里面读到dataframe里面, 多一列序号...

    试验发现:
    对于通过调用pd.read_csv('xxx.csv')返回dataframe, 会自动增加一列序号

    解决办法:
    调用read_csv时使用index_col参数设置当前第一列为index, 就不会再增加索引列了.
    pd.read_csv('xxx.csv', index_col=0)
    '''

    df1 = tushare.get_hist_data('600004', start='2016-07-18', end='2016-07-19')
    print('----------------- df1 -----------------')
    print(df1)
    df1.to_csv('result.csv')

    df2 = tushare.get_hist_data('600004', start='2016-07-20', end='2016-07-21')
    print('----------------- df2 -----------------')
    print(df2)

    old_data = pd.read_csv('result.csv', index_col=0)
    print(old_data)
    old_data.to_csv('result.csv')

    df3 = df2.append(old_data)
    print('----------------- df3 -----------------')
    print(df3)

    df3.to_csv('result.csv')


if __name__ == '__main__':
    dataframe2csv_test()