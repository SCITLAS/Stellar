# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


from stl_utils import stl_logger as slog

import tushare
import pandas as pd
import os


'''
获取宏观经济数据
'''


INFO_COUNT = 200              # 获取最新消息的条数
RETRY_COUNT = 5               # 调用tushare接口失败重试次数
RETRY_PAUSE = 0.1             # 调用tushare接口失败重试间隔时间

DEFAULT_DIR_PATH = '../../../Data/origin/tushare/security_social_news_data'


def get_news_data(count):
    '''
    获取即时财经新闻，类型包括国内财经、证券、外汇、期货、港股和美股等新闻信息

        classify :新闻类别
        title :新闻标题
        time :发布时间
        url :新闻链接
        content:新闻内容（在show_content为True的情况下出现）

    Parameters
    ------
        count:获取新闻条数
    return
    -------
        无
    '''
    dir_path = DEFAULT_DIR_PATH
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = '%s/news.csv' % dir_path

    tmp_data = pd.DataFrame()
    try:
        slog.StlDmLogger().debug('tushare.get_latest_news(%d)' % count)
        tmp_data = tushare.get_latest_news(top=count, show_content=False)
    except Exception as exception:
        slog.StlDmLogger().error('tushare.get_deposit_rate(%d) excpetion, args: %s' % (count, exception.args.__str__()))

    if tmp_data is None:
        slog.StlDmLogger().warning('tushare.get_deposit_rate(%d) return none' % count)
    else:
        tmp_data.to_csv(file_path)


def get_notice_data(code, date):
    '''
    获取个股信息地雷数据

        title:信息标题
        type:信息类型
        date:公告日期
        url:信息内容URL

    Parameters
    ------
        code:股票代码
        date:信息公布日期
    return
    -------
        无
    '''
    dir_path = '%s/notice' % DEFAULT_DIR_PATH
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = '%s/%s(%s).csv' % (dir_path, code, date)

    tmp_data = pd.DataFrame()
    try:
        slog.StlDmLogger().debug('tushare.get_notices(%s, %s)' % (code, date))
        tmp_data = tushare.get_notices(code=code, date=date)
    except Exception as exception:
        slog.StlDmLogger().error('tushare.get_notices(%s, %s) excpetion, args: %s' % (code, date, exception.args.__str__()))

    if tmp_data is None:
        slog.StlDmLogger().warning('tushare.get_notices(%s, %s) return none' % (code, date))
    else:
        tmp_data.to_csv(file_path)


def get_sina_guba_data():
    '''
    获取sina财经股吧首页的重点消息

        title, 消息标题
        content, 消息内容（show_content=True的情况下）
        ptime, 发布时间
        rcounts,阅读次数

    Parameters
    ------
        无
    return
    -------
        无
    '''
    dir_path = DEFAULT_DIR_PATH
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = '%s/sina_guba.csv' % dir_path

    tmp_data = pd.DataFrame()
    try:
        slog.StlDmLogger().debug('tushare.guba_sina(%s, %s)')
        tmp_data = tushare.guba_sina(show_content=False)
    except Exception as exception:
        slog.StlDmLogger().error('tushare.guba_sina() excpetion, args: %s' % exception.args.__str__())

    if tmp_data is None:
        slog.StlDmLogger().warning('tushare.guba_sina() return none')
    else:
        tmp_data.to_csv(file_path)


if __name__ == '__main__':
    get_news_data(INFO_COUNT)
    get_notice_data('002612', '2016-03-12')
    get_sina_guba_data()



