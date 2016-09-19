# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import codecs
import sys
import os
import shutil
import datetime
import tarfile
import errno
import requests
import click

from six import exec_, print_

from stl_strategy_simulator.test_back.comet.utils import dummy_func, convert_int_to_date
from stl_strategy_simulator.test_back.comet.analyser.data_proxy import LocalDataProxy
from stl_strategy_simulator.test_back.comet.simulator.scheduler import Scheduler
from stl_strategy_simulator.test_back.comet.simulator.trade_param import TradeSimulationParams
from stl_strategy_simulator.test_back.comet.simulator.trade_executor import TradeSimulationExecutor

from stl_utils.logger import bt_log
from stl_utils.logger import bt_user_print
from stl_utils.common import get_project_root

from stl_strategy_simulator.test_back.comet import api


'''
包装策略回测API
'''


DEFAULT_DATA_BUNDLE_PATH = ('%s/stl_strategy_simulator/test_back/comet/data_bundle/data' % get_project_root())
DEFAULT_DATA_BUNDLE_TMP_PATH = ('%s/stl_strategy_simulator/test_back/comet/data_bundle/tmp' % get_project_root())
DEFAULT_STRATEGY_PATH = ('%s/stl_strategy_simulator/test_back/comet/strategy' % get_project_root())
DEFAULT_RESULT_PATH = ('%s/stl_strategy_simulator/test_back/comet/result' % get_project_root())
DEFAULT_RESOURCE_PATH = ('%s/stl_strategy_simulator/test_back/comet/resource' % get_project_root())


def __get_data_bundle_path():
    if not os.path.exists(DEFAULT_DATA_BUNDLE_PATH):
        os.makedirs(DEFAULT_DATA_BUNDLE_PATH)
    return ('%s/comet.bundle' % DEFAULT_DATA_BUNDLE_PATH)


def __get_data_bundle_tmp_path():
    if not os.path.exists(DEFAULT_DATA_BUNDLE_TMP_PATH):
        os.makedirs(DEFAULT_DATA_BUNDLE_TMP_PATH)
    return ('%s/.comet' % DEFAULT_DATA_BUNDLE_TMP_PATH)


def __get_strategy_path(file_name):
    dir_path = DEFAULT_STRATEGY_PATH
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    return '%s/%s' % (dir_path, file_name)


def __get_result_path(dir):
    dir_path = '%s/%s' % (DEFAULT_RESULT_PATH, dir)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    return dir_path


def __get_resource_path():
    dir_path = DEFAULT_RESOURCE_PATH
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    return dir_path


def update_data_bundle(data_bundle_path=__get_data_bundle_path()):
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
    tmp = __get_data_bundle_tmp_path()

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

    results_df = run_strategy_simulation(source_code, strategy_file, start_date, end_date,
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


def run_strategy_simulation(source_code, strategy_filename, start_date, end_date,
                 init_cash, data_bundle_path, show_progress):
    scope = {
        "logger": bt_log,
        "print": bt_user_print,
    }
    scope.update({export_name: getattr(api, export_name) for export_name in api.__all__})
    code = compile(source_code, strategy_filename, 'exec')
    exec_(code, scope)

    try:
        data_proxy = LocalDataProxy(data_bundle_path)
    except OSError as e:
        if e.errno == errno.EEXIST:
            print_("data bundle might crash. Run `%s update_bundle` to redownload data bundle." % sys.argv[0])
            sys.exit()

    # FIXME set end_date to latest data's date
    dates = data_proxy.last("000001.XSHG", end_date, 10, "1d", "date")
    end_date = min(convert_int_to_date(dates[-1]), end_date)

    trading_cal = data_proxy.get_trading_dates(start_date, end_date)
    Scheduler.set_trading_dates(data_proxy.get_trading_dates(start_date, end_date.date()))
    trading_params = TradeSimulationParams(trading_cal, start_date=start_date.date(), end_date=end_date.date(),
                                   init_cash=init_cash, show_progress=show_progress)

    executor = TradeSimulationExecutor(
        init=scope.get("init", dummy_func),
        before_trading=scope.get("before_trading", dummy_func),
        handle_bar=scope.get("handle_bar", dummy_func),

        trading_params=trading_params,
        data_proxy=data_proxy,
    )

    results_df = executor.execute()

    return results_df


def show_draw_result(title, results_df):
    import matplotlib
    from matplotlib import gridspec
    import matplotlib.image as mpimg
    import matplotlib.pyplot as plt
    plt.style.use('ggplot')

    red = "#aa4643"
    blue = "#4572a7"
    black = "#000000"

    figsize = (18, 6)
    f = plt.figure(title, figsize=figsize)
    gs = gridspec.GridSpec(10, 8)

    # draw logo
    ax = plt.subplot(gs[:3, -1:])
    ax.axis("off")
    # filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), "resource")
    # filename = __get_resource_path()
    # filename = os.path.join(filename, "stellar.png")
    # img = mpimg.imread(filename)
    # imgplot = ax.imshow(img, interpolation="nearest")
    ax.autoscale_view()

    # draw risk and portfolio
    series = results_df.iloc[-1]

    font_size = 12
    value_font_size = 11
    label_height, value_height = 0.8, 0.6
    label_height2, value_height2 = 0.35, 0.15

    fig_data = [
        (0.00, label_height, value_height, "Total Returns", "{0:.3%}".format(series.total_returns), red, black),
        (0.15, label_height, value_height, "Annual Returns", "{0:.3%}".format(series.annualized_returns), red, black),
        (0.00, label_height2, value_height2, "Benchmark Total", "{0:.3%}".format(series.benchmark_total_returns), blue, black),
        (0.15, label_height2, value_height2, "Benchmark Annual", "{0:.3%}".format(series.benchmark_annualized_returns), blue, black),

        (0.30, label_height, value_height, "Alpha", "{0:.4}".format(series.alpha), black, black),
        (0.40, label_height, value_height, "Beta", "{0:.4}".format(series.beta), black, black),
        (0.55, label_height, value_height, "Sharpe", "{0:.4}".format(series.sharpe), black, black),
        (0.70, label_height, value_height, "Sortino", "{0:.4}".format(series.sortino), black, black),
        (0.85, label_height, value_height, "Information Ratio", "{0:.4}".format(series.information_rate), black, black),

        (0.30, label_height2, value_height2, "Volatility", "{0:.4}".format(series.volatility), black, black),
        (0.40, label_height2, value_height2, "MaxDrawdown", "{0:.3%}".format(series.max_drawdown), black, black),
        (0.55, label_height2, value_height2, "Tracking Error", "{0:.4}".format(series.tracking_error), black, black),
        (0.70, label_height2, value_height2, "Downside Risk", "{0:.4}".format(series.downside_risk), black, black),
    ]

    ax = plt.subplot(gs[:3, :-1])
    ax.axis("off")
    for x, y1, y2, label, value, label_color, value_color in fig_data:
        ax.text(x, y1, label, color=label_color, fontsize=font_size)
        ax.text(x, y2, value, color=value_color, fontsize=value_font_size)

    # strategy vs benchmark
    ax = plt.subplot(gs[4:, :])

    ax.get_xaxis().set_minor_locator(matplotlib.ticker.AutoMinorLocator())
    ax.get_yaxis().set_minor_locator(matplotlib.ticker.AutoMinorLocator())
    ax.grid(b=True, which='minor', linewidth=.2)
    ax.grid(b=True, which='major', linewidth=1)

    ax.plot(results_df["total_returns"], label="strategy", alpha=1, linewidth=2, color=red)
    ax.plot(results_df["benchmark_total_returns"], label="benchmark", alpha=1, linewidth=2, color=blue)

    # manipulate
    vals = ax.get_yticks()
    ax.set_yticklabels(['{:3.2f}%'.format(x*100) for x in vals])

    leg = plt.legend(loc="upper left")
    leg.get_frame().set_alpha(0.5)

    plt.show()


def run_simple_macd_strategy():
    para = {}
    para['strategy_file'] = __get_strategy_path('simple_macd.py')
    para['start_date'] = '2014-01-01'
    para['end_date'] = '2016-10-01'
    para['output_file'] = '%s/%s' % (__get_result_path('simple_macd'), 'result.pkl')
    para['data_bundle_path'] = __get_data_bundle_path()
    para['init_cash'] = 100000
    para['need_plot'] = True
    run(para)


def show_simple_macd_test_result():
    para = {}
    para['strategy_file'] = __get_strategy_path('simple_macd.py')
    para['start_date'] = '2014-01-01'
    para['end_date'] = '2016-10-01'
    para['output_file'] = '%s/%s' % (__get_result_path('simple_macd'), 'result.pkl')
    para['data_bundle_path'] = __get_data_bundle_path()
    para['init_cash'] = 100000
    para['need_plot'] = False
    df = run(para)
    show_result(para['strategy_file'], df)


if __name__ == '__main__':
    update_data_bundle()
    # run_simple_macd_strategy()
    show_simple_macd_test_result()