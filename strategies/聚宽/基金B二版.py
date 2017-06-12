import pandas as pd
import numpy as np
import datetime as dt
import math
import talib

def initialize(context):
    # 策略参考标准
    set_benchmark('000001.XSHG')
    g.origin='399001.XSHE'
    # 设置手续费，买入时万分之三，卖出时万分之三, 每笔交易最低扣5块钱
    set_commission(PerTrade(buy_cost=0.0003, sell_cost=0.0003, min_cost=5))
    g.s=False
    g.initial_price={}
    g.initial_amount={}
#---------------------------------
# 交易总次数(全部卖出时算一次交易)
    g.trade_total_count = 0
# 交易成功次数(卖出时盈利即算一次成功)
    g.trade_success_count = 0
# 统计数组(win存盈利的，loss 存亏损的)
    g.statis = {'win': [], 'loss': []}
#---------------------------------

def choose_stock(context,data):
    stockma=[]
    date=context.current_dt.date()
    stocksum=get_all_securities('fjb', date).index
    stocksum=unnew(stocksum,date)
    for stock in stocksum:
        his=attribute_history(stock, 6, '1d', ['close','volume'], skip_paused=True )
        if his['volume'][-1] >1000000:
           stockma.append(stock)
    return stockma
    
def delect_stock(stocks,data):
    if g.hour>=13:
        g.hour=g.hour-1.5
    ti=(g.hour-9)*60+(g.minute-30)
    for stock in stocks:
        count=30
        current_price=data[stock].close
        ma5=data[stock].mavg(5,'close')
        hiday = attribute_history(stock, 1, '1d', ['close','volume'],df=False)
        if current_price<ma5 or((current_price-hiday['close'][0])/hiday['close'][0])*100>9 \
            or ((current_price-hiday['close'][0])/hiday['close'][0])*100<-2 :
            g.security.remove(stock)
            continue
        #为了节省时间把上面这段先判断，过滤掉一半。
        hi=attribute_history(stock, ti, '1m', fields=('close', 'high', 'volume', 'money'),df=False)
        maxp=max(hi['high'])
        if ((maxp-current_price)/current_price)*100>1 \
            or sum(hi['volume'])<0.5*hiday['volume'] \
            or 100*(current_price-sum(hi['money'])/sum(hi['volume']))/hiday['close']>2:
            g.security.remove(stock)
            continue
        while count<ti:
            mp=sum(hi['money'][:count])/sum(hi['volume'][:count])
            if hi['close'][count]<mp :
               g.security.remove(stock)
               break
            count+=1
           
def sellstock(context,data):
    hold=list(context.portfolio.positions)
    current_data=get_current_data()
    for stock in hold:
        bottom_price= g.initial_price[stock]
        current_price = data[stock].close
        returns = (current_price-bottom_price)/bottom_price
        sellamount = context.portfolio.positions[stock].sellable_amount 
        if sellamount>0 and not current_data[stock].paused:
            sell_amount(context, data,stock, 0)
            log.info('卖出 %s :%.2f' % (str(cname(stock)),returns))
        
def buystock(context,data,buylist):
   #我要知道总市值现在是多少---------
    cash = context.portfolio.cash
    totol_value=context.portfolio.portfolio_value
    count=len(buylist)
    for stock in buylist:
        #我要买入3只票，总仓位为position()
        #buy_cash=totol_value*position(data)/3
        buy_cash=totol_value/count
        vol=int(buy_cash/data[stock].pre_close/100)*100
        #order_target(stock, vol)
        buy_amount(data,stock, vol)
        g.initial_price[stock]=data[stock].avg
        g.initial_amount[stock]=context.portfolio.positions[stock].total_amount
        log.info("买入 %s" % str(cname(stock))+'数量为'+str(g.initial_amount[stock]))
        
def setup_position(context,data,stock):
    bottom_price= g.initial_price[stock]
    if bottom_price == 0:
        return
    current_price = data[stock].close
    sellamount = context.portfolio.positions[stock].sellable_amount 
    returns = (current_price-bottom_price)/bottom_price
    if returns>0.05 and sellamount>0:
        sell_amount(context,data, stock, 0)
        log.info('卖出 %s :%.2f' % (str(cname(stock)),returns))
#===============================================================================   
def handle_data(context, data):
    g.hour = context.current_dt.hour
    g.minute = context.current_dt.minute
    #收盘全卖出-----------------------------------------
    if g.hour==14 and g.minute==56:
       sellstock(context,data)
    #每一分钟都对仓位进行调配--------------
    hold=list(context.portfolio.positions)
    for stock in hold:
        if context.portfolio.positions[stock].total_amount == 0:
           continue
        setup_position(context,data,stock)
    #每天早上删掉停牌ST次新股---------------------------
    if g.hour==9 and g.minute==31:
        g.security=choose_stock(context,data)
        set_universe(g.security)
    #当到达14点50分时，买入主力标的-------
    if g.hour==14 and g.minute==50:
        #先找出符合要求的标的：
        stocks=g.security[:]
        delect_stock(stocks,data)
        log.info('符合买入条件的股票数量为 %d' %(len(g.security)))
        record(Number=len(g.security))
        
        #然后根据需要买入
        hold=[stock for stock in list(context.portfolio.positions) if context.portfolio.positions[stock].total_amount>0]
        if len(hold)==3:
           return
        need_number=3-len(hold)
        buylist=g.security[:need_number]
        log.info(buylist)
        if data[g.origin].close>data[g.origin].mavg(1, 'close')*0.99:
            if data[g.origin].close/data[g.origin].mavg(1, 'close') < data['000001.XSHG'].close/data['000001.XSHG'].mavg(1, 'close')\
                and data['000001.XSHG'].close<0 :
                return
            elif len(g.security)>0:
                buystock(context,data,buylist)
#===================================收盘扫尾====================================
def after_trading_end(context):
    cash = context.portfolio.cash
    totol_value=context.portfolio.portfolio_value
    position=1-cash/totol_value
    log.info("收盘后持仓概况:%s" % cname(list(context.portfolio.positions)))
    log.info("仓位概况:%.2f" % position)
    print_win_rate(context.current_dt.strftime("%Y-%m-%d"), context.current_dt.strftime("%Y-%m-%d"), context)
    
#=============================================================================== 
#去除停牌的函数
def unpaused(stockspool):
    current_data=get_current_data()
    return [s for s in stockspool if not current_data[s].paused]

#去除ST的函数
def unst(stockspool):
    current_data=get_current_data()
    return [s for s in stockspool if not current_data[s].is_st]
    
#去除新股次新股的函数
def unnew(tockspool,date):
    df = get_all_securities('fjb')
    #print(df)
    one_year = dt.timedelta(50)
    df = df[df.start_date < date - one_year]
    #print(df).tail(50)
    return [s for s in tockspool if s in df.index]
    #print(unnew)
#股票中文名------------------------------------------
def cname(stocks):
    nn=''
    if type(stocks)==list:
       for stock in stocks:
           nn+='['+get_security_info(stock).display_name+stock+']'
       return nn
    else:
       return get_security_info(stocks).display_name+stocks
       
#收益计算----------------------------------------------------
# 买入指定数量股票
def buy_amount(data,stock, amount):
    current_data = get_current_data()
    price=current_data[stock].high_limit
    if 100 <= amount:
        order(stock, +amount, LimitOrderStyle(price))

# 卖出指定数量股票，若amount为0则表示清空该股票的所有持仓
def sell_amount(context,data, stock, amount):
    current_data = get_current_data()
    price=current_data[stock].low_limit
    if 0 == amount:
        record_trade_count(context, stock)
        __amount = context.portfolio.positions[stock].sellable_amount
        order_target_value(stock, 0, LimitOrderStyle(price))
    else:
        order(stock, -amount, LimitOrderStyle(price))

# 记录交易次数便于统计胜率
def record_trade_count(context, stock):
    g.trade_total_count += 1
    amount = context.portfolio.positions[stock].total_amount
    avg_cost = context.portfolio.positions[stock].avg_cost
    price = context.portfolio.positions[stock].last_sale_price

    current_value = amount * price
    cost = amount * avg_cost

    percent = round((current_value - cost) / cost * 100, 2)
    if current_value > cost:
        g.trade_success_count += 1
        win = [stock, percent]
        g.statis['win'].append(win)
    else:
        loss = [stock, percent]
        g.statis['loss'].append(loss)

# 打印胜率
def print_win_rate(current_date, print_date, context):
    if str(current_date) == str(print_date):
        win_rate = 0
        if 0 < g.trade_total_count and 0 < g.trade_success_count:
            win_rate = round(g.trade_success_count / float(g.trade_total_count), 2)

        most_win = statis_most_win_percent()
        most_loss = statis_most_loss_percent()
        starting_cash = context.portfolio.starting_cash
        total_profit = statis_total_profit(context)
        if len(most_win)==0 or len(most_loss)==0:
            return
        print "-"
        print '------------绩效报表------------'
        print '交易次数: {0}, 盈利次数: {1}, 胜率: {2}'.format(g.trade_total_count, g.trade_success_count, str(win_rate * 100) + str('%'))
        print '单次盈利最高: {0}, 盈利比例: {1}%'.format(most_win['stock'], most_win['value'])
        print '单次亏损最高: {0}, 亏损比例: {1}%'.format(most_loss['stock'], most_loss['value'])
        print '总资产: {0}, 本金: {1}, 盈利: {2}'.format(starting_cash + total_profit, starting_cash, total_profit)
        print '--------------------------------'
        print "-"

# 统计单次盈利最高的股票
def statis_most_win_percent():
    result = {}
    for statis in g.statis['win']:
        if {} == result:
            result['stock'] = statis[0]
            result['value'] = statis[1]
        else:
            if statis[1] > result['value']:
                result['stock'] = statis[0]
                result['value'] = statis[1]

    return result

# 统计单次亏损最高的股票
def statis_most_loss_percent():
    result = {}
    for statis in g.statis['loss']:
        if {} == result:
            result['stock'] = statis[0]
            result['value'] = statis[1]
        else:
            if statis[1] < result['value']:
                result['stock'] = statis[0]
                result['value'] = statis[1]

    return result

# 统计总盈利金额    
def statis_total_profit(context):
    return context.portfolio.portfolio_value - context.portfolio.starting_cash

