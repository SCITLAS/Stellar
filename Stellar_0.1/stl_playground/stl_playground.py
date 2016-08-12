# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


'''
轻量级的试验场地
'''


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
    mysql_test_2()

