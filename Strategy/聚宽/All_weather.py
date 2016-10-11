import math
import numpy as np
#from sklearn import preprocessing, cross_validation, svm
from sklearn import preprocessing, svm
import matplotlib.pyplot as plt
from matplotlib import style
import sklearn
import time
from datetime import datetime
import cPickle as pickle

def initialize(context):
    g.train=True
    
# year_date is for get_fundamentals
def train_data(year_date, index_date):
    Valuation=[]
    price=[]
    status=[]
    SZ1=get_index_stocks('399008.XSHE', date=index_date)
    SZ2=get_index_stocks('399012.XSHE', date=index_date)
    SH=get_index_stocks('399905.XSHE', date=index_date)
    tem_index=SZ1+SZ2+SH
    unix_30d=60*60*24*30
    unix_weekend=60*60*24*3
    
    q=query(
    income.code, income.pubDate, income.total_operating_revenue,
    income.total_operating_cost, income.administration_expense,
    income.operating_profit, income.non_operating_revenue,
    income.total_profit, income.net_profit, income.basic_eps,
    income.diluted_eps, income.total_composite_income
    ).filter(
    valuation.code.in_(tem_index))
    
    incm = get_fundamentals(q, statDate=year_date)
    date=incm['pubDate']
    index=incm['code']

    q=query(
    indicator
    ).filter(
    valuation.code.in_(index))
    indictor=get_fundamentals(q, statDate=year_date)
    del(indictor['code'], indictor['statDate'], indictor['pubDate'])

    for each in range(0,len(date)):
        q=query(
        valuation.pe_ratio, valuation.pb_ratio, valuation.circulating_market_cap
        ).filter(
        valuation.code==(index[each]))
        each_valuation=get_fundamentals(q, date=date[each])
        
        date_stamp = datetime.strptime(date[each], '%Y-%m-%d')
        unix=time.mktime(date_stamp.timetuple())
        unix_30_late=unix+unix_30d
        
        Valuation.append(each_valuation.iloc[0].tolist())
        
        p1=get_price(index[each], start_date=date[each], 
        end_date=date[each], frequency='daily', fields='close')
            
        if not p1.empty:
            pass
        else:
            p1_weekend=datetime.fromtimestamp(unix-unix_weekend).strftime('%Y-%m-%d')
            p1=get_price(index[each], start_date=p1_weekend, 
            end_date=p1_weekend, frequency='daily', fields='close')
            
        p1_30d=datetime.fromtimestamp(unix_30_late).strftime('%Y-%m-%d')
        p2=get_price(index[each], start_date=p1_30d, 
        end_date=p1_30d, frequency='daily', fields='close')
        
        if not p2.empty:
            pass
        else:
            date_stamp2 = datetime.strptime(p1_30d, '%Y-%m-%d')
            unix2=time.mktime(date_stamp2.timetuple())
            unix2_weekend=unix2-unix_weekend
            p2_weekend=datetime.fromtimestamp(unix2_weekend).strftime('%Y-%m-%d')
            p2=get_price(index[each], start_date=p2_weekend, 
            end_date=p2_weekend, frequency='daily', fields='close')

        dif =  p2.values / p1.values

        if dif > 1.1:
            s=1
        else:
            s=0
        status.append(s)
        
        price.append(p1.iloc[0].tolist())
            
    Valuation=pd.DataFrame(Valuation, columns=['pe','pb','cir_mkt_cap'])
    price=pd.DataFrame(price, columns=['price'])
    status=pd.DataFrame(status, columns=['status'])
    df=pd.concat([incm,Valuation,price,indictor,status], axis=1)
    
    del(df['pubDate'], df['statDate.1'], df['code'])
    #y=df['status'].values.tolist()
    #df=np.random.permutation(df)
    #del(df['status'], df['code'])
    #X=np.array(df.replace('NaN', 9999).values.tolist())
    #X=preprocessing.scale(X)
    return df

def fundamental(index):
    Valuation=[]
    price=[]
    status=[]
    
    q=query(
    income.total_operating_revenue,
    income.total_operating_cost, income.administration_expense,
    income.operating_profit, income.non_operating_revenue,
    income.total_profit, income.net_profit, income.basic_eps,
    income.diluted_eps, income.total_composite_income
    ).filter(
    valuation.code.in_(index))
    incm = get_fundamentals(q)
    
    q=query(
    valuation.pe_ratio, valuation.pb_ratio, valuation.circulating_market_cap
    ).filter(
    valuation.code.in_(index))
    Valuation=get_fundamentals(q)#.values.tolist()
    
    q=query(
    indicator
    ).filter(
    valuation.code.in_(index))
    indictor=get_fundamentals(q)#.values.tolist()
    index2=indictor['code']
    del(indictor['code'], indictor['statDate'], indictor['pubDate'], indictor['day'])
    
    for each in index2:
        p=attribute_history(each, 1, unit='1d', fields=['close'], skip_paused=True)
        price.append(p.iloc[0].tolist())
        
    price=pd.DataFrame(price, columns=['price'])
    df=pd.concat([incm,Valuation,price,indictor], axis=1)
    X=np.array(df.replace('NaN', 9999).values.tolist())

    X=preprocessing.scale(X)
    return X, index2

def handle_data(context, data):
    if g.train:
        index_date=str('2014-03-01')
        df1=train_data(str('2014q1'),index_date)
        df2=train_data(str('2014q2'),index_date)
        df3=train_data(str('2014q3'),index_date)
        df4=train_data(str('2014q4'),index_date)
        df=pd.concat([df1,df2,df3,df4], axis=0)
        df.iloc[np.random.permutation(len(df))]
        y=df['status'].values.tolist()
        del(df['status'])
        log.info("<===== shape of training dataset @ %s", str(df.shape))
        X=np.array(df.replace('NaN', 9999).values.tolist())

        X=preprocessing.scale(X)

        clf = svm.SVC(kernel=str("linear"), C=1.0)
        clf.fit(X, y)
        
        g.filename = "temp.pkl"
        pickle_file = open(g.filename, 'wb')
        pickle.dump(clf, pickle_file)
        pickle_file.close()
        g.train=False
    
    
    pickle_file = open(g.filename, 'rb')
    clf = pickle.load(pickle_file)
    
    year=context.current_dt.year
    month=context.current_dt.month
    day=context.current_dt.day
    index_date=str(year)+'-'+str(month)+'-'+str(day)

    SZ1=get_index_stocks('399008.XSHE', date=index_date)
    SZ2=get_index_stocks('399012.XSHE', date=index_date)
    SH=get_index_stocks('399905.XSHE', date=index_date)
    index=SZ1+SZ2+SH
    
    X, index2=fundamental(index)

    for each in range(0, len(index2)):
        if clf.predict(X[each].reshape(1,X.shape[1]))[0] == 1 and index2[each] not in context.portfolio.positions.keys():
            log.info("===================Buying:", index2[each])
            order_target_value(index2[each], context.portfolio.cash/5)
        if clf.predict(X[each].reshape(1,X.shape[1]))[0] == 0 and index2[each] in context.portfolio.positions.keys():
            log.info("<<<<<<<<<<<<<<<<<<Holding:", context.portfolio.positions.keys())
            log.info("-------------------selling:", index2[each])
            order_target(index2[each], 0)
        
# 止损
    if context.portfolio.positions:
        for stock in context.portfolio.positions.keys():
            cur_price = data[stock].close
            position=context.portfolio.positions[stock]
            if cur_price > position.avg_cost * (1 + 0.5) or cur_price < position.avg_cost * (1 - 0.2):
                order_target(stock, 0)
                log.info("<<<<<<<<<<<", stock, "%s lose:", 1-cur_price/position.avg_cost)

    




    