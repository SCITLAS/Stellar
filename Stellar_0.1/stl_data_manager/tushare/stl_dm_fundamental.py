# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


from stl_utils import stl_logger as slog
import os
import tushare
import pandas as pd


'''
获取证券股票的基本面信息
存入对应的csv文件
'''


def get_all_security_basic_info(refresh=False):
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
        refresh: 是否刷新
    return
    -------
        code_list: 股票代码列表
    '''
    dir_path = '../../../Data/origin/tushare/security_fundamental_data'
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = '%s/basic_info.csv' % dir_path

    if refresh:
        # 强制从tushare获取数据刷新
        basic_data = pd.DataFrame()
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
    else:
        # 不强制刷新
        if os.path.exists(file_path):
            # 文件存在, 就从文件里面读
            old_data = pd.read_csv(file_path)
            old_data['code'] = old_data['code'].apply(lambda x: '%06d' % x)
            return old_data['code'].tolist()
        else:
            # 文件不存在, 从tushare获取数据
            basic_data = pd.DataFrame()
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


if __name__ == "__main__":
    code_list_1 = get_all_security_basic_info(refresh=True)
    print('code_list_1:')
    print(code_list_1)

    code_list_2 = get_all_security_basic_info(refresh=False)
    print('code_list_2:')
    print(code_list_2)