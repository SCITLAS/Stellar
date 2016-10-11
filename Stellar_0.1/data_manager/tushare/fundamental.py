# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import os
import pandas as pd

import tushare

from utils.logger import dm_log
from utils.common import get_stellar_root


'''
获取证券股票的基本面信息
'''


# Global Consts
USING_CSV = 1
USING_MY_SQL = 2
USING_MONGO_DB = 3
STORAGE_MODE = USING_CSV

# TuShare Data Storage Path
DEFAULT_CSV_PATH_TS = ('%s/Data/csv/tushare' % get_stellar_root())
DEFAULT_MY_SQL_PATH_TS = ('%s/Data/mysql/tushare' % get_stellar_root())
DEFAULT_MONGO_DB_PATH_TS = ('%s/Data/mongodb/tushare' % get_stellar_root())

PROFIT_INFO_COUNT = 200       # 获取的分配预案数据条数
DATA_YEAR = 2016              # 获取数据的年份
DATA_QUARTER = 1              # 获取数据的季度
DATA_MONTH = 1                # 获取数据的月份

RETRY_COUNT = 5               # 调用tushare接口失败重试次数
RETRY_PAUSE = 0.1             # 调用tushare接口失败重试间隔时间


def get_directory_path():
    dir_path = ''
    if STORAGE_MODE == USING_CSV:
        dir_path = '%s/security_fundamental_data' % DEFAULT_CSV_PATH_TS
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    return dir_path


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
    if STORAGE_MODE == USING_CSV:
        return get_all_security_basic_info_csv(refresh)


def get_all_security_basic_info_csv(refresh=False):
    '''
    获取所有A股所有股票的基本信息存入csv

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
    file_path = '%s/basic.csv' % get_directory_path()
    if refresh:
        # 强制从tushare获取数据刷新
        try:
            dm_log.debug('tushare.get_stock_basics() called')
            basic_data = tushare.get_stock_basics()
        except Exception as exception:
            dm_log.error('tushare.get_stock_basics() excpetion, args: %s' % exception.args.__str__())
        else:
            if basic_data is None:
                dm_log.warning('tushare.get_stock_basics() return none')
                return []
            else:
                dm_log.debug('tushare.get_stock_basics() done, got %d rows' % len(basic_data))
                basic_data.to_csv(file_path)
                return basic_data.index.tolist()
    else:
        # 不强制刷新
        if os.path.exists(file_path):
            # 文件存在, 就从文件里面读
            old_data = pd.read_csv(file_path)
            old_data['code'] = old_data['code'].map(lambda x:str(x).zfill(6))
            return old_data['code'].tolist()
        else:
            # 文件不存在, 从tushare获取数据
            try:
                dm_log.debug('tushare.get_stock_basics() called')
                basic_data = tushare.get_stock_basics()
            except Exception as exception:
                dm_log.error('tushare.get_stock_basics() excpetion, args: %s' % exception.args.__str__())
            else:
                if basic_data is None:
                    dm_log.warning('tushare.get_stock_basics() return none')
                    return []
                else:
                    dm_log.debug('tushare.get_stock_basics() done, got %d rows' % len(basic_data))
                    basic_data.to_csv(file_path)
                    return basic_data.index.tolist()


def get_report_data(year, quarter):
    '''
    获取指定年份季度的股票的业绩报告(主表)

        code,代码
        name,名称
        eps,每股收益
        eps_yoy,每股收益同比(%)
        bvps,每股净资产
        roe,净资产收益率(%)
        epcf,每股现金流量(元)
        net_profits,净利润(万元)
        profits_yoy,净利润同比(%)
        distrib,分配方案
        report_date,发布日期

    Parameters
    ------
        year: 年度
        quarter: 季度
    return
    -------
        无
    '''
    try:
        dm_log.debug('tushare.get_report_data: year=%d, quarter=%d called' % (year, quarter))
        df = tushare.get_report_data(year=year, quarter=quarter)
    except Exception as exception:
        dm_log.error('tushare.get_report_data(%d, %d) excpetion, args: %s' % (year, quarter, exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.get_report_data(%d, %d) return none' % (year, quarter))
        else:
            dm_log.debug('tushare.get_report_data: year=%d, quarter=%d done, got %d rows' % (year, quarter, len(df)))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/report/%dQ%d.csv' % (get_directory_path(), year, quarter)
                df.to_csv(file_path)


def get_profit_data(year, quarter):
    '''
    获取指定年份季度的股票的盈利能力数据

        code,代码
        name,名称
        roe,净资产收益率(%)
        net_profit_ratio,净利率(%)
        gross_profit_rate,毛利率(%)
        net_profits,净利润(万元)
        eps,每股收益
        business_income,营业收入(百万元)
        bips,每股主营业务收入(元)

    Parameters
    ------
        year: 年度
        quarter: 季度
    return
    -------
        无
    '''
    try:
        dm_log.debug('tushare.get_profit_data: year=%d, quarter=%d called' % (year, quarter))
        df = tushare.get_profit_data(year=year, quarter=quarter)
    except Exception as exception:
        dm_log.error('tushare.get_profit_data(%d, %d) excpetion, args: %s' % (year, quarter, exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.get_profit_data(%d, %d) return none' % (year, quarter))
        else:
            dm_log.debug('tushare.get_profit_data: year=%d, quarter=%d done, got %d rows' % (year, quarter, len(df)))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/profit/%dQ%d.csv' % (get_directory_path(), year, quarter)
                df.to_csv(file_path)


def get_operation_data(year, quarter):
    '''
    获取指定年份季度的股票的运营能力数据

        code,代码
        name,名称
        arturnover,应收账款周转率(次)
        arturndays,应收账款周转天数(天)
        inventory_turnover,存货周转率(次)
        inventory_days,存货周转天数(天)
        currentasset_turnover,流动资产周转率(次)
        currentasset_days,流动资产周转天数(天)

    Parameters
    ------
        year: 年度
        quarter: 季度
    return
    -------
        无
    '''
    try:
        dm_log.debug('tushare.get_operation_data: year=%d, quarter=%d called' % (year, quarter))
        df = tushare.get_operation_data(year=year, quarter=quarter)
    except Exception as exception:
        dm_log.error('tushare.get_operation_data(%d, %d) excpetion, args: %s' % (year, quarter, exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.get_operation_data(%d, %d) return none' % (year, quarter))
        else:
            dm_log.debug('tushare.get_operation_data: year=%d, quarter=%d done, got %d rows' % (year, quarter, len(df)))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/operation/%dQ%d.csv' % (get_directory_path(), year, quarter)
                df.to_csv(file_path)


def get_growth_data(year, quarter):
    '''
    获取指定年份季度的股票的成长能力数据

        code,代码
        name,名称
        mbrg,主营业务收入增长率(%)
        nprg,净利润增长率(%)
        nav,净资产增长率
        targ,总资产增长率
        epsg,每股收益增长率
        seg,股东权益增长率

    Parameters
    ------
        year: 年度
        quarter: 季度
    return
    -------
        无
    '''
    try:
        dm_log.debug('tushare.get_growth_data: year=%d, quarter=%d called' % (year, quarter))
        df = tushare.get_growth_data(year=year, quarter=quarter)
    except Exception as exception:
        dm_log.error('tushare.get_growth_data(%d, %d) excpetion, args: %s' % (year, quarter, exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.get_growth_data(%d, %d) return none' % (year, quarter))
        else:
            dm_log.debug('tushare.get_growth_data: year=%d, quarter=%d done, got %d rows' % (year, quarter, len(df)))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/growth/%dQ%d.csv' % (get_directory_path(), year, quarter)
                df.to_csv(file_path)


def get_debt_data(year, quarter):
    '''
    获取指定年份季度的股票的偿债能力数据

        code,代码
        name,名称
        currentratio,流动比率
        quickratio,速动比率
        cashratio,现金比率
        icratio,利息支付倍数
        sheqratio,股东权益比率
        adratio,股东权益增长率

    Parameters
    ------
        year: 年度
        quarter: 季度
    return
    -------
        无
    '''
    try:
        dm_log.debug('tushare.get_debtpaying_data: year=%d, quarter=%d called' % (year, quarter))
        df = tushare.get_debtpaying_data(year=year, quarter=quarter)
    except Exception as exception:
        dm_log.error('tushare.get_debtpaying_data(%d, %d) excpetion, args: %s' % (year, quarter, exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.get_debtpaying_data(%d, %d) return none' % (year, quarter))
        else:
            dm_log.debug('tushare.get_debtpaying_data: year=%d, quarter=%d done, got %d rows' % (year, quarter, len(df)))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/debt_pay/%dQ%d.csv' % (get_directory_path(), year, quarter)
                df.to_csv(file_path)


def get_cashflow_data(year, quarter):
    '''
    获取指定年份季度的股票的现金流数据

        code,代码
        name,名称
        cf_sales,经营现金净流量对销售收入比率
        rateofreturn,资产的经营现金流量回报率
        cf_nm,经营现金净流量与净利润的比率
        cf_liabilities,经营现金净流量对负债比率
        cashflowratio,现金流量比率

    Parameters
    ------
        year: 年度
        quarter: 季度
    return
    -------
        无
    '''
    try:
        dm_log.debug('tushare.get_cashflow_data: year=%d, quarter=%d called' % (year, quarter))
        df = tushare.get_cashflow_data(year=year, quarter=quarter)
    except Exception as exception:
        dm_log.error('tushare.get_cashflow_data(%d, %d) excpetion, args: %s' % (year, quarter, exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.get_cashflow_data(%d, %d) return none' % (year, quarter))
        else:
            dm_log.debug('tushare.get_cashflow_data: year=%d, quarter=%d done, got %d rows' % (year, quarter, len(df)))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/cashflow/%dQ%d.csv' % (get_directory_path(), year, quarter)
                df.to_csv(file_path)


if __name__ == "__main__":
    code_list_1 = get_all_security_basic_info(refresh=True)
    print('code_list_1 (%d):' % len(code_list_1))
    print(code_list_1)

    code_list_2 = get_all_security_basic_info(refresh=False)
    print('code_list_2 (%d):' % len(code_list_2))
    print(code_list_2)

    get_report_data(year=DATA_YEAR, quarter=DATA_QUARTER)
    get_profit_data(year=DATA_YEAR, quarter=DATA_QUARTER)
    get_operation_data(year=DATA_YEAR, quarter=DATA_QUARTER)
    get_growth_data(year=DATA_YEAR, quarter=2)
    get_debt_data(year=DATA_YEAR, quarter=DATA_QUARTER)
    get_cashflow_data(year=DATA_YEAR, quarter=DATA_QUARTER)