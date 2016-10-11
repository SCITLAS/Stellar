__author__ = 'MoroJoJo'
# coding = utf-8


import os

import tushare

from utils.logger import dm_log
from utils.common import get_stellar_root


'''
获取龙虎榜数据
'''


# Global Consts
USING_CSV = 1
USING_MY_SQL = 2
USING_MONGO_DB = 3
STORAGE_MODE = USING_CSV

# TuShare Data Storage Path
DEFAULT_CSV_PATH_TS = ('%s/Data/csv/tushare' % get_stellar_root())
DEFAULT_MY_SQL_PATH_TS = ('../../../Data/mysql/tushare' % get_stellar_root())
DEFAULT_MONGO_DB_PATH_TS = ('../../../Data/mongodb/tushare' % get_stellar_root())

PROFIT_INFO_COUNT = 200       # 获取的分配预案数据条数
DATA_YEAR = 2016              # 获取数据的年份
DATA_QUARTER = 1              # 获取数据的季度
DATA_MONTH = 1                # 获取数据的月份

RETRY_COUNT = 5               # 调用tushare接口失败重试次数
RETRY_PAUSE = 0.1             # 调用tushare接口失败重试间隔时间


def get_directory_path():
    dir_path = ''
    if STORAGE_MODE == USING_CSV:
        dir_path = '%s/security_rank_data' % DEFAULT_CSV_PATH_TS
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    return dir_path


def get_rank_top_data(date):
    '''
    获取指定日期的龙虎榜列表

        code：代码
        name:名称
        pchange:当日涨跌幅
        amount：龙虎榜成交额(万)
        buy：买入额(万)
        bratio：买入占总成交比例
        sell：卖出额(万)
        sratio：卖出占总成交比例
        reason：上榜原因
        date：日期

    Parameters
    ------
        date: 日期
    return
    -------
        无
    '''
    try:
        dm_log.debug('tushare.top_list(%s) called' % date)
        df = tushare.top_list(date=date, retry_count=RETRY_COUNT, pause=RETRY_PAUSE)
    except Exception as exception:
        dm_log.error('tushare.top_list(%s) excpetion, args: %s' % (date, exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.top_list(%s) return none' % date)
        else:
            dm_log.debug('tushare.top_list(%s) done, got %d rows' % (date, len(df)))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/top_list/%s.csv' % (get_directory_path(), date)
                df.to_csv(file_path)


def get_cap_top_data(days):
    '''
    获取个股上榜统计数据

        code：代码
        name:名称
        count：上榜次数
        bamount：累积购买额(万)
        samount：累积卖出额(万)
        net：净额(万)
        bcount：买入席位数
        scount：卖出席位数

    Parameters
    ------
        days: 天数，统计n天以来上榜次数，默认为5天，其余是10、30、60
    return
    -------
        无
    '''
    try:
        dm_log.debug('tushare.cap_tops(%d) called' % days)
        df = tushare.cap_tops(days=days, retry_count=RETRY_COUNT, pause=RETRY_PAUSE)
    except Exception as exception:
        dm_log.error('tushare.cap_tops(%d) excpetion, args: %s' % (days, exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.cap_tops(%d) return none' % days)
        else:
            dm_log.debug('tushare.cap_tops(%d) done, got %d rows' % (days, len(df)))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/top_statistics/%ddays.csv' % (get_directory_path(), days)
                df.to_csv(file_path)


def get_broker_tops_data(days):
    '''
    获取营业部上榜统计数据

        broker：营业部名称
        count：上榜次数
        bamount：累积购买额(万)
        bcount：买入席位数
        samount：累积卖出额(万)
        scount：卖出席位数
        top3：买入前三股票

    Parameters
    ------
        days: 统计n天以来上榜次数，默认为5天，其余是10、30、60
    return
    -------
        无
    '''
    try:
        dm_log.debug('tushare.broker_tops(%d) called' % days)
        df = tushare.broker_tops(days=days, retry_count=RETRY_COUNT, pause=RETRY_PAUSE)
    except Exception as exception:
        dm_log.error('tushare.broker_tops(%d) excpetion, args: %s' % (days, exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.broker_tops(%d) return none' % days)
        else:
            dm_log.debug('tushare.broker_tops(%d) done, got %d rows' % (days, len(df)))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/broker/top_%ddays.csv' % (get_directory_path(), days)
                df.to_csv(file_path)


def get_inst_tops_data(days):
    '''
    获取机构席位追踪统计数据

        code:代码
        name:名称
        bamount:累积买入额(万)
        bcount:买入次数
        samount:累积卖出额(万)
        scount:卖出次数
        net:净额(万)

    Parameters
    ------
        days: 统计n天以来上榜次数，默认为5天，其余是10、30、60
    return
    -------
        无
    '''
    try:
        dm_log.debug('tushare.inst_tops(%d) called' % days)
        df = tushare.inst_tops(days=days, retry_count=RETRY_COUNT, pause=RETRY_PAUSE)
    except Exception as exception:
        dm_log.error('tushare.inst_tops(%d) excpetion, args: %s' % (days, exception.args.__str__()))
    else:
        if df is None:
            dm_log.warning('tushare.inst_tops(%d) return none' % days)
        else:
            dm_log.debug('tushare.inst_tops(%d) done, got %d rows' % (days, len(df)))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/institution/top_%ddays.csv' % (get_directory_path(), days)
                df.to_csv(file_path)


def get_inst_detail_data():
    '''
    获取机构席位追踪统计数据

        code:代码
        name:名称
        date:交易日期
        bamount:机构席位买入额(万)
        samount:机构席位卖出额(万)
        type:类型

    Parameters
    ------
        无
    return
    -------
        无
    '''
    try:
        dm_log.debug('tushare.inst_detail() called')
        df = tushare.inst_detail(retry_count=RETRY_COUNT, pause=RETRY_PAUSE)
    except Exception as exception:
        dm_log.error('tushare.inst_detail() excpetion, args: %s' % exception.args.__str__())
    else:
        if df is None:
            dm_log.warning('tushare.inst_detail() return none')
        else:
            dm_log.debug('tushare.inst_detail() done, got %d rows' % len(df))
            if STORAGE_MODE == USING_CSV:
                file_path = '%s/institution/detail.csv' % get_directory_path()
                df.to_csv(file_path)


if __name__ == '__main__':
    get_rank_top_data('2016-08-03')
    get_cap_top_data(5)
    get_broker_tops_data(10)
    get_inst_tops_data(15)
    get_inst_detail_data()

