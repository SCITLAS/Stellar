from datetime import timedelta, date
import pandas as pd

def initialize(context):
    log.info('Computing...')
    set_benchmark('000300.XSHG')
    set_commission(PerTrade(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
    set_slippage(PriceRelatedSlippage())
    g.stocknum = 10 # 持股数
    
    ## 自动设定调仓月份（如需使用自动，注销下段）
    i = 1  # 调仓频率
    log.info(range(1,13,12/i))
    g.Transfer_date = range(1,13,12/i)
    
    ## 手动设定调仓月份（如需使用手动，注销上段）
    # g.Transfer_date = (3,9)
    
    ## 按月调用程序
    run_monthly(Transfer,15)

def Transfer(context):
    months = context.current_dt.month
    if months in g.Transfer_date:
        ## 分配资金
        if len(context.portfolio.positions) < g.stocknum :
            Num = g.stocknum  - len(context.portfolio.positions)
            Cash = context.portfolio.cash/Num
        else: 
            Cash = context.portfolio.cash
        
        ##获得Buylist
        Buy_stock = Value_stock(context)
        # Buylist = sorted(Buy_stock.iteritems(),key=lambda t:t[1],reverse=False)
        Buylist = list(Buy_stock.keys())
        # print Buylist
        
        ## 卖出
        if len(context.portfolio.positions) > 0:
            for stock in context.portfolio.positions.keys():
                if stock not in Buylist:
                    order_target(stock, 0)
        ## 买入
        if len(Buylist) > 0:
            for stock in Buylist:
               if stock not in context.portfolio.positions.keys():
                   order_value(stock,Cash)
    else:
        pass
    
def Value_stock(context):
    current_date = context.current_dt.date()
    time1 = current_date - timedelta(days = 365)
    time2 = current_date - 2*timedelta(days = 365)
    time3 = current_date - 3*timedelta(days = 365)
    ## 获取近几年的EPS，并用近三年的历史增长率代表预期收益增长率
    df = get_fundamentals(query(
        income.code,
        income.day,
        income.operating_revenue,
        income.basic_eps
    ),date = current_date)
    t = list(df['code'])
    df1 = get_fundamentals(query(
        income.basic_eps
    ).filter(
        income.code.in_(t)
    ),date = time1)
    df2 = get_fundamentals(query(
        income.basic_eps
    ).filter(
        income.code.in_(t)
    ),date = time2)
    df3 = get_fundamentals(query(
        income.basic_eps
    ).filter(
        income.code.in_(t)
    ),date = time3)
    dnf = pd.concat([df,df1,df2,df3],axis = 1).dropna()
    tt = list(dnf['code'])
    dnf.columns = ['code','R','Value','basic_eps','basic_eps1','basic_eps2','basic_eps3']
    # 计算预期收益增长率
    dnf['R'] = ((dnf['basic_eps']/dnf['basic_eps1']-1) \
        +(dnf['basic_eps1']/dnf['basic_eps2']-1) \
        +(dnf['basic_eps2']/dnf['basic_eps3']-1))/3
    # 计算成长股内在价值Value
    dnf['Value'] = dnf['basic_eps']*(8.5 + 2*dnf['R'])
    V = dict(zip(dnf['code'],dnf['Value']))
    ## 获取Value/Price 在1到1.2之间的股票
    h = history(1, '1d', 'price', security_list=tt, df=False)
    Buy_stock = {}
    for stock in tt:
        if 1< V[stock]/h[stock][0] <1.2:
            Buy_stock[stock] = V[stock]/h[stock][0]
    return Buy_stock
    
    
    
