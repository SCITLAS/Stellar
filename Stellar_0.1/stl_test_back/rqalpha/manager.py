# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import codecs
import sys
import os
import shutil
import datetime
import tarfile

import click
import requests
from six import print_

from rqalpha.__main__ import run_strategy
from rqalpha.__main__ import show_draw_result


'''
基于RiceQuant开源的策略回测框架RQAlpha,包装的策略回测API
'''


DEFAULT_DATA_BUNDLE_PATH = './data_bundle/data'
DEFAULT_DATA_BUNDLE_TMP_PATH = './data_bundle/tmp'
DEFAULT_STRATEGY_PATH = './strategy'
DEFAULT_RESULT_PATH = './result'


def _get_data_bundle_path():
    if not os.path.exists(DEFAULT_DATA_BUNDLE_PATH):
        os.makedirs(DEFAULT_DATA_BUNDLE_PATH)
    return ('%s/rq.bundle' % DEFAULT_DATA_BUNDLE_PATH)


def _get_data_bundle_tmp_path():
    if not os.path.exists(DEFAULT_DATA_BUNDLE_TMP_PATH):
        os.makedirs(DEFAULT_DATA_BUNDLE_TMP_PATH)
    return ('%s/.rqalpha' % DEFAULT_DATA_BUNDLE_TMP_PATH)


def _get_strategy_path(type, file_name):
    if type == 'rqalpha':
        dir_path = '%s/%s' % (DEFAULT_STRATEGY_PATH, 'rqalpha')
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        return '%s/%s' % (dir_path, file_name)
    else:
        dir_path = '%s/%s' % (DEFAULT_STRATEGY_PATH, 'stellar')
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        return '%s/%s' % (dir_path, file_name)


def _get_result_path(dir):
    dir_path = '%s/%s' % (DEFAULT_RESULT_PATH, dir)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    return dir_path


def update_data_bundle(data_bundle_path=_get_data_bundle_path()):
    '''
    更新回测行情历史数据

    Parameters
    ------
        无
    return
    -------
        无
    '''
    """update data bundle, download if not found"""
    day = datetime.date.today()
    tmp = _get_data_bundle_tmp_path()

    while True:
        url = 'http://7xjci3.com1.z0.glb.clouddn.com/bundles/rqbundle_%04d%02d%02d.tar.bz2' % (day.year, day.month, day.day)
        print_('try {} ...'.format(url))
        r = requests.get(url, stream=True)
        if r.status_code != 200:
            day = day - datetime.timedelta(days=1)
            continue

        out = open(tmp, 'wb')
        total_length = int(r.headers.get('content-length'))

        with click.progressbar(length=total_length, label='downloading ...') as bar:
            for data in r.iter_content(chunk_size=8192):
                bar.update(len(data))
                out.write(data)

        out.close()
        break

    shutil.rmtree(data_bundle_path, ignore_errors=True)
    os.mkdir(data_bundle_path)
    tar = tarfile.open(tmp, 'r:bz2')
    tar.extractall(data_bundle_path)
    tar.close()
    os.remove(tmp)


def run(para):
    '''
    运行策略回测

    Parameters
    ------
        para: 参数dict
    return
    -------
        无
    '''
    strategy_file = para['strategy_file']
    start_date_str = para['start_date']
    start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date_str = para['end_date']
    end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d')
    output_file = para['output_file']
    data_bundle_path = para['data_bundle_path']
    init_cash = para['init_cash']
    need_plot = para['need_plot']

    if not os.path.exists(data_bundle_path):
        print_("data bundle not found. Run `%s update_bundle` to download data bundle." % sys.argv[0])
        return

    with codecs.open(strategy_file, encoding="utf-8") as f:
        source_code = f.read()

    results_df = run_strategy(source_code, strategy_file, start_date, end_date,
                              init_cash, data_bundle_path, need_plot)

    if output_file is not None:
        results_df.to_pickle(output_file)

    if need_plot:
        show_draw_result(strategy_file, results_df)

    print(results_df)
    return results_df


def show_result(strategy_file, result_df):
    '''
    图形化形式策划回测结果

    Parameters
    ------
        strategy_file: 策略文件
        result_df: 策略回测结果
    return
    -------
        无
    '''
    show_draw_result(strategy_file, result_df)


def run_simple_macd_strategy():
    para = {}
    para['strategy_file'] = _get_strategy_path('rqalpha', 'simple_macd.py')
    para['start_date'] = '2014-01-01'
    para['end_date'] = '2016-10-01'
    para['output_file'] = '%s/%s' % (_get_result_path('simple_macd'), 'result.pkl')
    para['data_bundle_path'] = _get_data_bundle_path()
    para['init_cash'] = 100000
    para['need_plot'] = True
    run(para)


def show_simple_macd_test_result():
    para = {}
    para['strategy_file'] = _get_strategy_path('rqalpha', 'simple_macd.py')
    para['start_date'] = '2014-01-01'
    para['end_date'] = '2016-10-01'
    para['output_file'] = '%s/%s' % (_get_result_path('simple_macd'), 'result.pkl')
    para['data_bundle_path'] = _get_data_bundle_path()
    para['init_cash'] = 100000
    para['need_plot'] = False
    df = run(para)
    show_result(para['strategy_file'], df)


if __name__ == '__main__':
    # update_data_bundle()
    # run_simple_macd_strategy()
    show_simple_macd_test_result()





