import pandas as pd
import numpy as np

def initialize(context):
    g.choicenum = 1 #预选小市值股票数
    g.days = 0 # 计时器
    g.runned_years = set([])
    g.runned_seasons = set([])

    # 现在应该持有的仓位, 因为可能停牌导致卖出不成功, 跟实际持仓可能不一样
    g.stocks_to_hold = set()

    # 调仓频率
    g.period = 5 # 'week', 'month', 'season','year'

    # 是否止损
    g.should_stop_loss = True #True:开启止损,False:关闭止损

    # 如果是按周调仓,
    if g.period == 'week':
        run_weekly(rebalance, 1)
    elif g.period in ('month', 'year', 'season'):
        run_monthly(monthly, 1)
    else:
        run_daily(daily)

    # 防止卖出不成功, 每日尝试卖出
    # run_daily(clean_stocks_to_sell)

    if g.should_stop_loss:
        run_daily(stop_loss)
        
def daily(context):
    if g.days % g.period == 0:
        rebalance(context)
    g.days += 1

def monthly(context):
    if g.period == 'month':
        rebalance(context)
    elif g.period == 'year':
        year = context.current_dt.year
        if year not in g.runned_years:
            # 这一年没运行过, 运行一次
            g.runned_years.add(year)
            rebalance(context)
    elif g.period == 'season':
        year = context.current_dt.year
        # 取得月份对应的季度, 1-3月->1季度, 4-6月->2季度, ...
        season = (context.current_dt.month - 1) / 3 + 1
        year_season = (year, season)
        if year_season not in g.runned_seasons:
            # 这一年的这一季度没运行过, 运行一次
            g.runned_seasons.add(year_season)
            rebalance(context)
    pass

# 调整
def rebalance(context):
    print 'rebalance at %s' % context.current_dt
    # 设置沪深两市所有股票为股票池
    scu0 = get_index_stocks('000001.XSHG')
    scu3 = get_index_stocks('399106.XSHE')
    scu = scu0+scu3
    
    # scu = scu[:10]
    set_universe(scu)

    date=context.current_dt.strftime("%Y-%m-%d")

    # 选出低市值的股票，buylist
    df = get_fundamentals(query(
            valuation.code,valuation.market_cap
        ).filter(
            valuation.code.in_(context.universe)
        ).order_by(
            valuation.market_cap.asc()
        ), date=date
        ).dropna()
    # 去除停牌
    buylist =unpaused(list(df['code']))
    # 去除ST，*ST
    st=get_extras('is_st', buylist, start_date=date, end_date=date, df=True)
    st=st.loc[date]
    buylist=list(st[st==False].index)
    # print list(st[st==False].index[:g.choicenum])
    
    g.stocks_to_hold =buylist[:g.choicenum]
    print g.stocks_to_hold

    clean_stocks_to_sell(context)

    # 等权重买入buylist中的股票
    position_per_stk = context.portfolio.cash/g.choicenum
    closes = history(1, '1d', 'price', df=False)
    for stock in g.stocks_to_hold:
        close = closes[stock][-1]
        if not isnan(close):
            amount = int(position_per_stk/close)
            order(stock, +amount)
    set_universe(g.stocks_to_hold)
    
# 清空应该卖出的股票
def clean_stocks_to_sell(context):
    for stock in context.portfolio.positions:
        if stock not in g.stocks_to_hold:
            order_target(stock, 0)

# 止损
def stop_loss(context):
    for stock in context.portfolio.positions:
        p = context.portfolio.positions[stock]
        if p.price/p.avg_cost < 0.9:
            order_target(stock,0)
            if stock in g.stocks_to_hold:
                g.stocks_to_hold.remove(stock)


def unpaused(stockspool):
    current_data=get_current_data()
    return [s for s in stockspool if not current_data[s].paused]