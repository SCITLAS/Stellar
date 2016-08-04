# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


from stl_utils import stl_logger as slog

import tushare
import pandas as pd
import os


'''
获取股票分类数据
'''


RETRY_COUNT = 5               # 调用tushare接口失败重试次数
RETRY_PAUSE = 0.1             # 调用tushare接口失败重试间隔时间

DEFAULT_DIR_PATH = '../../../Data/origin/tushare/security_classify_data'


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
    dir_path = DEFAULT_DIR_PATH
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = '%s/industry_classify.csv' % dir_path

    tmp_data = pd.DataFrame()
    try:
        slog.StlDmLogger().debug('tushare.get_industry_classified(%s)' % source)
        tmp_data = tushare.get_industry_classified(standard=source)
    except Exception as exception:
        slog.StlDmLogger().error('tushare.get_industry_classified(%s) excpetion, args: %s' % (source, exception.args.__str__()))

    if tmp_data is None:
        slog.StlDmLogger().warning('tushare.get_industry_classified(%s) return none' % source)
    else:
        tmp_data.to_csv(file_path)


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
    dir_path = DEFAULT_DIR_PATH
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = '%s/concept_classify.csv' % dir_path

    tmp_data = pd.DataFrame()
    try:
        slog.StlDmLogger().debug('tushare.get_industry_classified()')
        tmp_data = tushare.get_concept_classified()
    except Exception as exception:
        slog.StlDmLogger().error('tushare.get_industry_classified() excpetion, args: %s' % (exception.args.__str__()))

    if tmp_data is None:
        slog.StlDmLogger().warning('tushare.get_industry_classified() return none')
    else:
        tmp_data.to_csv(file_path)


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
    dir_path = DEFAULT_DIR_PATH
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = '%s/area_classify.csv' % dir_path

    tmp_data = pd.DataFrame()
    try:
        slog.StlDmLogger().debug('tushare.get_area_classified()')
        tmp_data = tushare.get_area_classified()
    except Exception as exception:
        slog.StlDmLogger().error('tushare.get_area_classified() excpetion, args: %s' % (exception.args.__str__()))

    if tmp_data is None:
        slog.StlDmLogger().warning('tushare.get_area_classified() return none')
    else:
        tmp_data.to_csv(file_path)


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
    dir_path = DEFAULT_DIR_PATH
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = '%s/sme_classify.csv' % dir_path

    tmp_data = pd.DataFrame()
    try:
        slog.StlDmLogger().debug('tushare.get_sme_classified()')
        tmp_data = tushare.get_sme_classified()
    except Exception as exception:
        slog.StlDmLogger().error('tushare.get_sme_classified() excpetion, args: %s' % (exception.args.__str__()))

    if tmp_data is None:
        slog.StlDmLogger().warning('tushare.get_sme_classified() return none')
    else:
        tmp_data.to_csv(file_path)


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
    dir_path = DEFAULT_DIR_PATH
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = '%s/gem_classify.csv' % dir_path

    tmp_data = pd.DataFrame()
    try:
        slog.StlDmLogger().debug('tushare.get_gem_classified()')
        tmp_data = tushare.get_gem_classified()
    except Exception as exception:
        slog.StlDmLogger().error('tushare.get_gem_classified() excpetion, args: %s' % (exception.args.__str__()))

    if tmp_data is None:
        slog.StlDmLogger().warning('tushare.get_gem_classified() return none')
    else:
        tmp_data.to_csv(file_path)


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
    dir_path = DEFAULT_DIR_PATH
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = '%s/st_classify.csv' % dir_path

    tmp_data = pd.DataFrame()
    try:
        slog.StlDmLogger().debug('tushare.get_st_classified()')
        tmp_data = tushare.get_st_classified()
    except Exception as exception:
        slog.StlDmLogger().error('tushare.get_st_classified() excpetion, args: %s' % (exception.args.__str__()))

    if tmp_data is None:
        slog.StlDmLogger().warning('tushare.get_st_classified() return none')
    else:
        tmp_data.to_csv(file_path)


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
    dir_path = DEFAULT_DIR_PATH
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = '%s/hs300_classify.csv' % dir_path

    tmp_data = pd.DataFrame()
    try:
        slog.StlDmLogger().debug('tushare.get_hs300s')
        tmp_data = tushare.get_hs300s()
    except Exception as exception:
        slog.StlDmLogger().error('tushare.get_hs300s() excpetion, args: %s' % (exception.args.__str__()))

    if tmp_data is None:
        slog.StlDmLogger().warning('tushare.get_hs300s() return none')
    else:
        tmp_data.to_csv(file_path)


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
    dir_path = DEFAULT_DIR_PATH
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = '%s/sz50_classify.csv' % dir_path

    tmp_data = pd.DataFrame()
    try:
        slog.StlDmLogger().debug('tushare.get_sz50s()')
        tmp_data = tushare.get_sz50s()
    except Exception as exception:
        slog.StlDmLogger().error('tushare.get_sz50s() excpetion, args: %s' % (exception.args.__str__()))

    if tmp_data is None:
        slog.StlDmLogger().warning('tushare.get_sz50s() return none')
    else:
        tmp_data.to_csv(file_path)


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
    dir_path = DEFAULT_DIR_PATH
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = '%s/zz500_classify.csv' % dir_path

    tmp_data = pd.DataFrame()
    try:
        slog.StlDmLogger().debug('tushare.get_zz500s()')
        tmp_data = tushare.get_zz500s()
    except Exception as exception:
        slog.StlDmLogger().error('tushare.get_zz500s() excpetion, args: %s' % (exception.args.__str__()))

    if tmp_data is None:
        slog.StlDmLogger().warning('tushare.get_zz500s() return none')
    else:
        tmp_data.to_csv(file_path)


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
    dir_path = DEFAULT_DIR_PATH
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = '%s/terminated_classify.csv' % dir_path

    tmp_data = pd.DataFrame()
    try:
        slog.StlDmLogger().debug('tushare.get_terminated()')
        tmp_data = tushare.get_terminated()
    except Exception as exception:
        slog.StlDmLogger().error('tushare.get_terminated() excpetion, args: %s' % (exception.args.__str__()))

    if tmp_data is None:
        slog.StlDmLogger().warning('tushare.get_terminated() return none')
    else:
        tmp_data.to_csv(file_path)


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
    dir_path = DEFAULT_DIR_PATH
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = '%s/suspended_classify.csv' % dir_path

    tmp_data = pd.DataFrame()
    try:
        slog.StlDmLogger().debug('tushare.get_suspended()')
        tmp_data = tushare.get_suspended()
    except Exception as exception:
        slog.StlDmLogger().error('tushare.get_suspended() excpetion, args: %s' % (exception.args.__str__()))

    if tmp_data is None:
        slog.StlDmLogger().warning('tushare.get_suspended() return none')
    else:
        tmp_data.to_csv(file_path)


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

