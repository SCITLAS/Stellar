import numpy as np
def initialize(context):
    g.up_price = {} #向上碎形最高价
    g.low_price = {} #向下碎形最低价
    g.up_fractal_exists = {} #判断有效向上碎形
    g.down_fractal_exists = {} #判断有效向下碎形
    g.AO_index = {} #存放连续的AO指标数据
    g.cal_AC_index = {} #计算AC指标中转存储
    g.AC_index = {} #存放连续的AC指标数据
    g.amount = {} #满仓仓位
    g.stock = get_index_stocks('000300.XSHG')
    g.buy_stock = []
    set_benchmark('000300.XSHG')
    g.month = context.current_dt.month
    run_monthly(select_universe,1,'open')

#重置全局变量
def reset_global():
    g.up_price = {} #向上碎形最高价
    g.low_price = {} #向下碎形最低价
    g.up_fractal_exists = {} #判断有效向上碎形
    g.down_fractal_exists = {} #判断有效向下碎形
    g.AO_index = {} #存放连续的AO指标数据
    g.cal_AC_index = {} #计算AC指标中转存储
    g.AC_index = {} #存放连续的AC指标数据
    g.amount = {} #满仓仓位
    g.buy_stock = []

def initial_stock_global(stock):
    g.up_price[stock] = 0
    g.low_price[stock] = 0
    g.up_fractal_exists[stock] = False
    g.down_fractal_exists[stock] = False #判断有效向下碎形
    g.AO_index[stock] = [0] #存放连续的AO指标数据
    g.cal_AC_index[stock] = [0]  #计算AC指标中转存储
    g.AC_index[stock] = [0] #存放连续的AC指标数据
    g.amount[stock] = 0 #满仓仓位

#轮换选股后清空持仓
def reset_position(context):
    for stock in g.buy_stock:
        order_target(stock,0)
        log.info("sell %s for reset position"%stock)
#选股
def select_universe(context):
    #每三个月操作一次
    month = context.current_dt.month
    if month%6 != g.month%6:
        return
    #清空全局变量
    reset_position(context)
    reset_global()
    hist = history(30,'1d','close',g.stock,df = False)
    for stock in g.stock:
        if is_sleeping_alligator(stock,hist,20):
            g.buy_stock.append(stock)
            #初始化该股票全局变量
            initial_stock_global(stock)
    print g.buy_stock
    return None

#睡着的鳄鱼
def is_sleeping_alligator(stock,hist,nday):
    for i in range(nday):
        if is_struggle(stock,hist,i) == False:
            return False
    return True

#均线纠缠，BRG三线非常接近
def is_struggle(stock,hist,delta):
    blue_line = hist[stock][-21-delta:-8-delta].mean()
    red_line = hist[stock][-13-delta:-5-delta].mean()
    green_line = hist[stock][-8-delta:-3-delta].mean()
    if abs(blue_line/red_line-1)<0.02 and abs(red_line/green_line-1)<0.02:
        return True
    else:
        return False

#判断 向上 或 向下 碎形
def is_fractal(stock,direction):
    hist = history(5,'1d',direction,[stock],df = False)
    if direction == 'high'\
    and hist[stock][2] > hist[stock][0]\
    and hist[stock][2] > hist[stock][1]\
    and hist[stock][2] > hist[stock][3]\
    and hist[stock][2] > hist[stock][4]:
        g.up_price[stock] = hist[stock][2]
        return True
    elif direction == 'low'\
    and hist[stock][2] < hist[stock][0]\
    and hist[stock][2] < hist[stock][1]\
    and hist[stock][2] < hist[stock][3]\
    and hist[stock][2] < hist[stock][4]:
        g.low_price[stock] = hist[stock][2]
        return True
    return False

#通过比较碎形与红线位置，判断碎形是否有效
def is_effective_fractal(stock, direction):
    if is_fractal(stock,direction):
        hist = history(13,'1d','close',[stock],df = False)
        red_line = hist[stock][:-5].mean()
        close_price = hist[stock][-1]
        if direction == 'high':
            if close_price > red_line:
                g.up_fractal_exists[stock] = True
            else:
                g.up_fractal_exists[stock] = False
        elif direction == 'low':
            if close_price < red_line:
                g.down_fractal_exists[stock] = True
            else:
                g.down_fractal_exists[stock] = False

#N日内最高价格的N日线
def nday_high_point(stock,n):
    hist = history(2*n,'1d','high',[stock],df = False)[stock]
    high_point = []
    for i in range(n):
        high_point.append(max(hist[-5-i:-1-i]))
    return np.array(high_point).mean()

#N日内最低价格的N日线
def nday_low_point(stock,n):
    hist = history(2*n,'1d','low',[stock],df = False)[stock]
    low_point = []
    for i in range(n):
        low_point.append(max(hist[-5-i:-1-i]))
    return np.array(low_point).mean()

#AO=5日内（最高-最低）/2的5日移动平均-34日内（最高-最低）/2的34日移动平均
def AO_index(stock):
    g.AO_index[stock].append(nday_high_point(stock,5)/2 + nday_low_point(stock,5)/2\
                      - nday_high_point(stock,34)/2 - nday_low_point(stock,34)/2)
    return None

#AO-AO的5日平均值的5日平均
def AC_index(stock):
    AO_index(stock)
    if len(g.AO_index[stock]) >= 5:
        g.cal_AC_index[stock].append(g.AO_index[stock][-1] - np.array(g.AO_index[stock][-5:]).mean())
        if len(g.cal_AC_index[stock]) >=5:
            g.AC_index[stock].append(np.array(g.cal_AC_index[stock][-5:]).mean())

#判断序列n日上行
def is_up_going(alist,n):
    if len(alist) < n:
        return False
    for i in range(n-1):
        if alist[-(1+i)] <= alist[-(2+i)]:
            return False
    return True

#判断序列n日下行
def is_down_going(alist,n):
    if len(alist) < n:
        return False
    for i in range(n-1):
        if alist[-(1+i)] >= alist[-(2+i)]:
            return False
    return True

#碎形被突破
def active_fractal(stock,direction):
    close_price = history(1,'1d','close',[stock],df=False)[stock][0]
    if direction == 'up' and close_price > g.up_price[stock]:
        return True
    elif direction == 'down' and close_price < g.low_price[stock]:
        return True
    return False

#进场，初始仓位
def set_initial_position(stock,context):
    close_price = history(1,'1d','close',[stock],df=False)[stock][0]
    g.amount[stock] = context.portfolio.cash/close_price/len(g.buy_stock)*3
    order(stock, g.amount[stock])
    log.info("buying %s 股数为 %s"%(stock,g.amount[stock]))
    g.down_fractal_exists[stock] = False

#卖出
def sell_all_stock(stock,context):
    order_target(stock,0)
    log.info("selling %s"%stock)
    g.up_fractal_exists[stock] = False

#加仓
def adjust_position(stock,context,position):
    order(stock,g.amount[stock]*position)
    log.info("adjust position buying %s 股数为 %s"%(stock,g.amount[stock]*position))

# 计算股票前n日收益率
def security_return(days,security_code):
    hist1 = attribute_history(security_code, days + 1, '1d', 'close',df=False)
    security_returns = (hist1['close'][-1]-hist1['close'][0])/hist1['close'][0]
    return security_returns

# 止损，根据前n日收益率
def conduct_nday_stoploss(context,security_code,days,bench):
    if  security_return(days,security_code)<= bench:
        for stock in g.buy_stock:
            order_target_value(stock,0)
            log.info("Sell %s for stoploss" %stock)
        return True
    else:
        return False

# 计算股票累计收益率（从建仓至今）
def security_accumulate_return(context,data,stock):
    current_price = data[stock].price
    cost = context.portfolio.positions[stock].avg_cost
    if cost != 0:
        return (current_price-cost)/cost
    else:
        return None

# 个股止损，根据累计收益
def conduct_accumulate_stoploss(context,data,stock,bench):
    if security_accumulate_return(context,data,stock) != None\
    and security_accumulate_return(context,data,stock) < bench:
        order_target_value(stock,0)
        log.info("Sell %s for stoploss" %stock)
        return True
    else:
        return False

# 个股止盈，根据累计收益
def conduct_accumulate_stopwin(context,data,stock,bench):
    if security_accumulate_return(context,data,stock) != None\
    and security_accumulate_return(context,data,stock) > bench:
        order_target_value(stock,0)
        log.info("Sell %s for stopwin" %stock)
        return True
    else:
        return False

def handle_data(context,data):
    #大盘止损
    if conduct_nday_stoploss(context,'000300.XSHG',3,-0.03):
        return
    for stock in g.buy_stock:
        #个股止损
        if conduct_accumulate_stopwin(context,data,stock,0.3)\
        or conduct_accumulate_stoploss(context,data,stock,-0.1):
            return
        #计算AO，AC指标
        AC_index(stock)
        #空仓时，寻找机会入场
        if context.portfolio.positions[stock].amount == 0:
            #计算向上碎形
            is_effective_fractal(stock,'high')
            #有效向上碎形存在，并被突破，买入
            if g.up_fractal_exists and active_fractal(stock,'up'):
                close_price = history(5, '1d', 'close', [stock],df = False)
                if is_up_going(g.AO_index[stock],5)\
                and is_up_going(g.AC_index[stock],3)\
                and is_up_going(close_price[stock],2):
                    set_initial_position(stock,context)
        #有持仓时，加仓或离场
        else:
            #计算向下碎形
            is_effective_fractal(stock,'low')
            #出场条件1：有效向下碎形存在，并被突破，卖出
            if g.down_fractal_exists and active_fractal(stock,'down'):
                sell_all_stock(stock,context)
                return
            #出场条件2：
            #加仓10%：AO，AC同时5日上行，且收盘价走高
            # if is_up_going(g.AO_index[stock],5)\
            # and is_up_going(g.AC_index[stock],3)\
            # and is_up_going(close_price[stock],2):
            #     adjust_position(stock,context,0.1)
            # #减仓10%：AO，AC同时3日下行，且收盘价走低
            # if is_down_going(g.AO_index[stock],5)\
            # and is_down_going(g.AC_index[stock],3)\
            # and is_down_going(close_price[stock],2):
            #     adjust_position(stock,context,-0.1)