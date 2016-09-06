# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


from time import time
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from contextlib import contextmanager


import pandas as pd
import threadpool
import tushare as ts


'''
轻量级的试验场地1号
'''


# dataframe test
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
    import tushare

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

def dataframe2list_test():
    '''
    把dataframe的index列保存到一个list里面
    '''
    import tushare

    data = tushare.get_stock_basics()
    data_list = data.index.tolist()
    print(data_list)


def _map(data, exp):
    for index, row in data.iterrows():   # 获取每行的index、row
        for col_name in data.columns:
            row[col_name] = exp(row[col_name]) # 把结果返回给data
    return data

def _1map(data, exp):
    _data = [[exp(row[col_name])               # 把结果转换成2级list
             for col_name in data.columns]
             for index, row in data.iterrows()
            ]
    return _data






# mysql test
def mysql_test():
    import mysql.connector

    conn = mysql.connector.connect(user='root', password='QWERasdf2013root', database='Stellar', use_unicode=True)

    cusor = conn.cursor()

    cusor.execute('create table user (id varchar(20) primary key, name varchar(20))')
    cusor.execute('insert into user (id, name) values (%s, %s)', ['1', 'MoroJoJo'])
    print(cusor.rowcount)

    conn.commit()

    cusor.execute('select * from user where id = %s', ('1',))
    values = cusor.fetchall()
    print(values)

    cusor.close()
    conn.close()

def mysql_test_2():
    from sqlalchemy import Column, String, create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.ext.declarative import declarative_base
    import tushare as ts

    # # 创建对象的基类:
    # Base = declarative_base()
    #
    # # 定义User对象:
    # class User(Base):
    #     # 表的名字:
    #     __tablename__ = 'user'
    #
    #     # 表的结构:
    #     id = Column(String(20), primary_key=True)
    #     name = Column(String(20))

    # 初始化数据库连接:
    # engine = create_engine('mysql+mysqlconnector://root:QWERasdf2013root@localhost:3306/Stellar')
    # 创建DBSession类型:
    # DBSession = sessionmaker(bind=engine)

    # df = ts.get_tick_data('600848', date='2014-12-22')
    # engine = create_engine('mysql+mysqlconnector://root:QWERasdf2013root@localhost:3306/Stellar')
    engine = create_engine('mysql+mysqlconnector://root:QWERasdf2013root@127.0.0.1:3306/Stellar?charset=utf8')
    #
    # #存入数据库
    # df.to_sql('tick_data',engine)






# Multi thread and no multi thread performance test
def factorize(number):
    for i in range(1, number+1):
        if number % i == 0:
            yield i


def factorize_no_multi_thread_test():
    numbers = [2139079, 1214759, 1516637, 1852285]
    start = time()
    for number in numbers:
        list(factorize(number))
    end = time()
    print('No multi thread test took %.3f seconds' % (end - start))


class FactorizeThread(Thread):
    def __init__(self, number):
        super().__init__()
        self.number = number

    def run(self):
        self.factors = list(factorize(self.number))


def factorize_multi_thread_test():
    numbers = [2139079, 1214759, 1516637, 1852285]
    start = time()
    threads = []
    for number in numbers:
        thread = FactorizeThread(number)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
    end = time()
    print('Multi thread test took %.3f seconds' % (end - start))


def do_factorize_thread_test():
    factorize_no_multi_thread_test()
    factorize_multi_thread_test()






# Tushare thread and no multi thread performance test
def get_trade_history(code):
    df = ts.get_hist_data(code, start='2000-01-01', end='2016-08-20')
    print(len(df))


def ts_no_multi_thread_test():
    codes = ['000951', '600036', '600100', '600577']
    start = time()
    for code in codes:
        get_trade_history(code)
    end = time()
    print('Tushare no multi thread test took %.3f seconds' % (end - start))


class TushareThread(Thread):
    def __init__(self, code):
        super().__init__()
        self.code = code

    def run(self):
        get_trade_history(self.code)


def ts_multi_thread_test():
    codes = ['000951', '600036', '600100', '600577']
    start = time()
    threads = []
    for code in codes:
        thread = TushareThread(code)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
    end = time()
    print('Tushare multi thread test took %.3f seconds' % (end - start))


def do_ts_thread_test():
    ts_no_multi_thread_test()
    ts_multi_thread_test()






# thread pool test
def do_ts_threadpool_test():
    # ts_no_multi_thread_test()
    ts_threadpool_test()


def ts_single_thread_test():
    code_list = ['000951', '600036', '600100', '600577']
    start = time()
    for code in code_list:
        get_day_data(code)
    end = time()
    print('Tushare single thread test took %.3f seconds' % (end - start))


def ts_threadpool_test():
    code_list = ['000951', '600036', '600100', '600577']
    start = time()
    pool = threadpool.ThreadPool(10)
    requests = threadpool.makeRequests(callable_=get_day_data, args_list=code_list, callback=print_result)
    [pool.putRequest(req) for req in requests]
    pool.wait()

    pool2 = threadpool.ThreadPool(10)
    requests2 = threadpool.makeRequests(callable_=get_week_data, args_list=code_list, callback=print_result)
    [pool2.putRequest(req) for req in requests2]
    pool2.wait()

    end = time()
    print('Tushare threadpool test took %.3f seconds' % (end - start))


def get_day_data(code):
    df = ts.get_hist_data(code=code, ktype='D')
    print(len(df))

def get_week_data(code):
    df = ts.get_hist_data(code=code, ktype='W')
    print(len(df))


def print_result(request, result):
    print("the result is %s %r"%(request.requestID, result))






# concurrent.futures test
def do_ts_thread_pool_executor_test():
    code_list = ['000951', '600036', '600100', '600577']
    start = time()
    pool = ThreadPoolExecutor(max_workers=2)
    pool.map(get_day_data, code_list)
    end = time()
    print('Tushare threadpool test took %.3f seconds' % (end - start))






# coroutine test
# def coroutine_produce(consumer):
#     code_list = ['000951', '600036', '600100', '600577']
#     next(consumer)
#     index = 0
#     while index < len(code_list):
#         print('[PRODUCER] Producing %s...' % index)
#         result = consumer.send(code_list[index])
#         print('[PRODUCER] Consumer return: %s' % result)
#         index = index + 1
#     consumer.close()
#
#
# def coroutine_consume():
#     result = 0
#     while True:
#         code = yield result
#         if not code:
#             return
#         print('[CONSUMER] Consuming %s...' % code)
#         df = ts.get_h_data(code)
#         print(len(df))
#
# def do_ts_coroutine_test():
#     start = time()
#     c = coroutine_consume()
#     coroutine_produce(c)
#     end = time()
#     print('Tushare coroutine test took %.3f seconds' % (end - start))

def ts_coroutine():
    while True:
        code = yield
        df = ts.get_h_data(code, '2000-01-01', '2016-08-10')
        print(len(df))

def do_ts_coroutine_test():
    code_list = ['000951', '600036', '600100', '600577']
    it = ts_coroutine()
    next(it)
    for code in code_list:
        print('send code: %s' % code)
        it.send(code)







def wrap_me(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        print('1.in wrapper, before call func')
        result = func(*args, **kwargs)
        print('3.in wrapper, after call func')
        print('4.func result: %s' % result)
        return result
    return wrapper

@wrap_me
def some_func():
    print('2.some_func called')
    return 'some_func result'

def wrap_test():
    some_func()






@contextmanager
def do_something(para):
    print('1.do_something with para:%s' % para)
    try:
        yield
    finally:
        print('3.do_something end')

@contextmanager
def do_something_2(para):
    print('1.do_something with para:%s' % para)
    ds = object
    try:
        yield ds
    finally:
        print('3.do_something end')

def contextmanager_test():
    print('---------------------------------------------')
    with do_something('xxx'):
        print('2.Inside')
    print('4.Outside')

    print('---------------------------------------------')

    with do_something_2('xxx') as ds:
        print('2.Inside, ds is %s' % ds)
    print('4.Outside')
    print('---------------------------------------------')







def matplotlib_test():
    # import matplotlib.pyplot as plot
    # for index, color in enumerate('rgbyck'):
    #     plot.subplot(321+index, axisbg=color)
    # plot.show()

    import matplotlib.pyplot as plt
    import numpy as np

    x = np.linspace(-4, 4, 200)
    f1 = np.power(10, x)
    f2 = np.power(np.e, x)
    f3 = np.power(2, x)

    plt.plot(x, f1, 'r', x, f2, 'b', x, f3, 'g', linewidth=2)
    plt.axis([-4, 4, -0.5, 8])
    plt.text(1, 7.5, r'$10^x$', fontsize=16)
    plt.text(2.2, 7.5, r'$e^x$', fontsize=16)
    plt.text(3.2, 7.5, r'$2^x$', fontsize=16)
    plt.title('A simple example', fontsize=20)

    plt.savefig('power.png', dpi=75)
    plt.show()









if __name__ == '__main__':
    # dataframe2csv_test()
    # dataframe2list_test()

    # inp = [{'c1':10, 'c2':100}, {'c1':11,'c2':110}, {'c1':12,'c2':120}]
    # df = pd.DataFrame(inp)
    # print(df)
    # temp = _map(df, lambda ele: ele+1)
    # temp = _map(df, lambda ele: ele)
    # print(temp)

    # _temp = _1map(df, lambda ele: ele+1)
    # res_data = pd.DataFrame(_temp)         # 对2级list转换成DataFrame
    # print(res_data)

    # mysql_test()
    # mysql_test_2()

    # do_factorize_thread_test()
    # do_ts_thread_test()

    # do_ts_threadpool_test()
    # do_ts_thread_pool_executor_test()

    # do_ts_coroutine_test()

    # wrap_test()
    # contextmanager_test()

    matplotlib_test()




