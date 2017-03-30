# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import os

import tushare

from code.utils.logger import dm_log
from code.utils.common import get_stellar_root


'''
获取股票分类数据
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


def get_directory_path():
    dir_path = ''
    if STORAGE_MODE == USING_CSV:
        dir_path = '%s/security_classify_data' % DEFAULT_CSV_PATH_TS
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    return dir_path


def get_industry_classify_data(source='sina'):
    '''
    获取股票行业分类信息

        code：股票代码
        name：股票名称
        c_name：行业名称

    Parameters
    ------
        source:信息来源, sina:新浪行业 sw：申万 行业
    -------
        无
    '''
    try:
        dm_log.debug('tushare.get_industry_classified(%s) called' % source)
        df = tushare.get_industry_classified(standard=source)
    except Exception as exception:
        dm_log.error('tushare.get_industry_classified(%s) excpetion, args: %s' % (source, exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.get_industry_classified(%s) return none' % source)
        else:
            dm_log.debug('tushare.get_industry_classified(%s) done, got %d rows' % (source, len(df)))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/industry.csv' % get_directory_path()
                df.to_csv(file_path)


def get_concept_classify_data():
    '''
    获取股票概念分类信息

        code：股票代码
        name：股票名称
        c_name：概念名称

    Parameters
    ------
        无
    -------
        无
    '''
    try:
        dm_log.debug('tushare.get_industry_classified() called')
        df = tushare.get_concept_classified()
    except Exception as exception:
        dm_log.error('tushare.get_industry_classified() excpetion, args: %s' % (exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.get_industry_classified() return none')
        else:
            dm_log.debug('tushare.get_industry_classified() done, got %d rows' % len(df))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/concept.csv' % get_directory_path()
                df.to_csv(file_path)


def get_area_classify_data():
    '''
    获取股票地域分类信息

        code：股票代码
        name：股票名称
        area：地域名称

    Parameters
    ------
        无
    -------
        无
    '''
    try:
        dm_log.debug('tushare.get_area_classified() called')
        df = tushare.get_area_classified()
    except Exception as exception:
        dm_log.error('tushare.get_area_classified() excpetion, args: %s' % (exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.get_area_classified() return none')
        else:
            dm_log.debug('tushare.get_area_classified() done, got %d rows' % len(df))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/area.csv' % get_directory_path()
                df.to_csv(file_path)


def get_sme_classify_data():
    '''
    获取中小板股票信息

        code：股票代码
        name：股票名称

    Parameters
    ------
        无
    -------
        无
    '''
    try:
        dm_log.debug('tushare.get_sme_classified() called')
        df = tushare.get_sme_classified()
    except Exception as exception:
        dm_log.error('tushare.get_sme_classified() excpetion, args: %s' % (exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.get_sme_classified() return none')
        else:
            dm_log.debug('tushare.get_sme_classified() done, got %d rows' % len(df))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/sme.csv' % get_directory_path()
                df.to_csv(file_path)


def get_gem_classify_data():
    '''
    获取中小板股票信息

        code：股票代码
        name：股票名称

    Parameters
    ------
        无
    -------
        无
    '''
    try:
        dm_log.debug('tushare.get_gem_classified() called')
        df = tushare.get_gem_classified()
    except Exception as exception:
        dm_log.error('tushare.get_gem_classified() excpetion, args: %s' % (exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.get_gem_classified() return none')
        else:
            dm_log.debug('tushare.get_gem_classified() done, got %d rows' % len(df))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/gem.csv' % get_directory_path()
                df.to_csv(file_path)


def get_st_classify_data():
    '''
    获取风险警示板股票信息

        code：股票代码
        name：股票名称

    Parameters
    ------
        无
    -------
        无
    '''
    try:
        dm_log.debug('tushare.get_st_classified() called')
        df = tushare.get_st_classified()
    except Exception as exception:
        dm_log.error('tushare.get_st_classified() excpetion, args: %s' % (exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.get_st_classified() return none')
        else:
            dm_log.debug('tushare.get_st_classified() done, got %d rows' % len(df))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/st.csv' % get_directory_path()
                df.to_csv(file_path)


def get_hs300_classify_data():
    '''
    获取沪深300成份股和权重信息

        code：股票代码
        name：股票名称
        date :日期
        weight:权重

    Parameters
    ------
        无
    -------
        无
    '''
    try:
        dm_log.debug('tushare.get_hs300s called')
        df = tushare.get_hs300s()
    except Exception as exception:
        dm_log.error('tushare.get_hs300s() excpetion, args: %s' % (exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.get_hs300s() return none')
        else:
            dm_log.debug('tushare.get_hs300s done, got %d rows' % len(df))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/hs300.csv' % get_directory_path()
                df.to_csv(file_path)


def get_sz50_classify_data():
    '''
    获取上证50成份股信息

        code：股票代码
        name：股票名称

    Parameters
    ------
        无
    -------
        无
    '''
    try:
        dm_log.debug('tushare.get_sz50s() called')
        df = tushare.get_sz50s()
    except Exception as exception:
        dm_log.error('tushare.get_sz50s() excpetion, args: %s' % (exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.get_sz50s() return none')
        else:
            dm_log.debug('tushare.get_sz50s() done, got %d rows' % len(df))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/sz50.csv' % get_directory_path()
                df.to_csv(file_path)


def get_zz500_classify_data():
    '''
    获取中证500成份股信息

        code：股票代码
        name：股票名称

    Parameters
    ------
        无
    -------
        无
    '''
    try:
        dm_log.debug('tushare.get_zz500s() called')
        df = tushare.get_zz500s()
    except Exception as exception:
        dm_log.error('tushare.get_zz500s() excpetion, args: %s' % (exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.get_zz500s() return none')
        else:
            dm_log.debug('tushare.get_zz500s() done, got %d rows' % len(df))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/zz500.csv' % get_directory_path()
                df.to_csv(file_path)


def get_terminated_classify_data():
    '''
    获取终止上市股票信息(tushare接口目前只有在上海证券交易所交易被终止上市的股票。)

        code：股票代码
        name：股票名称

    Parameters
    ------
        无
    -------
        无
    '''
    try:
        dm_log.debug('tushare.get_terminated() called')
        df = tushare.get_terminated()
    except Exception as exception:
        dm_log.error('tushare.get_terminated() excpetion, args: %s' % (exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.get_terminated() return none')
        else:
            dm_log.debug('tushare.get_terminated() done, got %d rows' % len(df))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/terminated.csv' % get_directory_path()
                df.to_csv(file_path)


def get_suspended_classify_data():
    '''
    获取暂停上市股票信息(tushare接口目前只有在上海证券交易所交易被暂停上市的股票。)

        code：股票代码
        name：股票名称

    Parameters
    ------
        无
    -------
        无
    '''
    try:
        dm_log.debug('tushare.get_suspended() called')
        df = tushare.get_suspended()
    except Exception as exception:
        dm_log.error('tushare.get_suspended() excpetion, args: %s' % (exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.get_suspended() return none')
        else:
            dm_log.debug('tushare.get_suspended() done, got %d rows' % len(df))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/suspended.csv' % get_directory_path()
                df.to_csv(file_path)


if __name__ == '__main__':
    get_industry_classify_data()
    get_concept_classify_data()
    get_area_classify_data()
    get_sme_classify_data()
    get_gem_classify_data()
    get_st_classify_data()
    get_hs300_classify_data()
    get_sz50_classify_data()
    get_zz500_classify_data()
    get_suspended_classify_data()
    get_terminated_classify_data()

