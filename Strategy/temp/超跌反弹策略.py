from datetime import timedelta, date

def initialize(context):
    set_benchmark('000300.XSHG')
    set_commission(PerTrade(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
    g.stocknum = 3 # 持股数
    # set_option('use_real_price', True)
    log.set_level('order', 'error')
    g.one_day = timedelta(days = 1)
    g.per_sell_stock = [] #欲卖出股票列表
    g.proportion_initial_buy_cash = 0.6 #建仓资金占总资金的比例
    g.proportion_cash = 0.4 #预留补仓现金比例占总资金的比例
    #（0.6代表用仓位总资金的六成用于建仓，其余四成用于加仓）
    initialize_position_cash(context) #初始化仓位个数及资金
    check_stocks(context) #初始筛选股票
    g.i = 0
    ## 执行函数
    #股票池更新
    # run_daily(check_stocks, time='before_open')
    run_weekly(check_stocks,1,time='before_open') 
    # run_monthly(check_stocks, 1, time='before_open')
    # 获取买入列表
    run_daily(get_buylist,time='before_open')
    # 买入股票
    run_daily(buy_stocks, time='open')
    # 加仓与止盈
    run_daily(overweight_and_stop_profit, time='open')
    # 止损
    run_daily(stop_loss, time='open')
    # 更新仓位信息
    run_daily(change_position_cash, time='after_close')

def check_stocks(context):
    # 选出所有的总市值小于限定值的股票
    df = get_fundamentals(query(
            valuation.code, valuation.market_cap
        ).filter(
            valuation.market_cap < 80
        ).order_by(
            valuation.market_cap.asc()
        )).dropna()

    code1 = list(df['code'])
    
    all_stock = get_all_securities(['stock'])
    # 不要创业板股票
    stocks_300 = [code for code in all_stock.index if code.upper().startswith('300')]
    code2 = [s for s in code1 if s not in stocks_300]
    # 获取ST股票
    ST = [code for code in all_stock.index if all_stock['display_name'][code].upper().startswith('*') or all_stock['display_name'][code].upper().startswith('ST')]
    # 提出ST股票
    code3 = [s for s in code2 if s not in ST]
    # 选取上面的结果作为universe
    g.security = code3 
    # log.info(len(g.security))
    if len(g.security) == 0:
        log.info("未筛选出股票")
        g.i = 1
        return
    else:
        g.i = 0
        set_universe(g.security)

def get_buylist(context):
    if g.i == 1:
        check_stocks(context)
        if g.i == 1:
            return
        else:
            pass
    
    g.already_hold_stock = context.portfolio.positions.keys()
    g.buylist = []
    for stock in g.security:
        if stock not in g.already_hold_stock:
            df = attribute_history(stock, 15, '1d', ['close','low','open'], df=False)
            if df['open'][-1] < df['close'][-1]:
                count = 0
                for i in range(1, 11):
                    if df['open'][-i] > df['close'][-i]:
                        count += 1
                if count >= 6:
                    g.buylist.append(stock)
    log.info("len of buylist: %i", len(g.buylist))

def initialize_position_cash(context):
    '''
    初始化每个仓位的信息
    '''
    start_cash = context.portfolio.starting_cash
    every_position = start_cash/g.stocknum
    g.hold_temp = {'total':every_position, 'initial_buy_cash':every_position*g.proportion_initial_buy_cash, 'cash':every_position*g.proportion_cash}
    g.hold = {} # 仓位信息
    g.hold_stock_name = {} # 仓位对于的股票名称
    for i in range(g.stocknum):
        g.hold[i] = g.hold_temp.copy() 
        g.hold_stock_name[i] = None #仓位为空，则对应的value值为None
    # log.info('init hold:',g.hold)
    # log.info('hold_stock_name:',g.hold_stock_name)

def buy_stocks(context):
    current_data = get_current_data(g.buylist)
    for stock in g.buylist:
        if current_data[stock].paused == 0:#跳过停牌
            for n in g.hold_stock_name.items():
                if n[1] == None:
                    g.hold_stock_name[n[0]] = stock #标记仓位对应的股票
                    Cash = g.hold[n[0]]['initial_buy_cash'] #获取建仓资金
                    order_value(stock, Cash) #建仓
                    # log.info('buy: %s',stock)
                    g.buylist.remove(stock)
                    break
                else:
                    pass
        else:
            g.buylist.remove(stock)
    # log.info('hold_stock_name:',g.hold_stock_name)
def overweight_and_stop_profit(context):
    '''
    加仓以及止盈
    '''
    hold_stock = context.portfolio.positions.keys()
    if len(hold_stock)>0:
        current_data = get_current_data(hold_stock)
        for stock in hold_stock:
            #跳过停牌，因为T+1。所以sellable_amount>0即跳过当日建仓的股票
            if current_data[stock].paused == 0 and context.portfolio.positions[stock].sellable_amount>0:
                avg_cost = context.portfolio.positions[stock].avg_cost #持仓成本
                price = context.portfolio.positions[stock].price #持仓股票当前价
                # 根据触发条件，对不在“欲卖出”列表的股票进行加仓，降低持仓成本
                if ((price/avg_cost) <= 0.8) and (stock not in g.per_sell_stock):
                    log.info("overweight: %s", stock) #打印加仓股票代码
                    i = get_key(stock) #获取加仓股票对应的key值，get_key函数见下
                    Cash = get_buy_cash(i) #获取用于加仓资金，get_buy_cash函数见下
                    order_value(stock, Cash) #买入
                    # log.info('buy: %s',stock)
                # 止盈
                elif (price/avg_cost) >= 1.2:
                    order_target(stock, 0)
                    log.info("Stop_profit: %s, avg_cost=%.2f, price=%.2f", stock, avg_cost, price)
                    # 如果股票在“欲卖出”列表中，则将其删除
                    if stock in g.per_sell_stock:
                        g.per_sell_stock.remove(stock)
            else:
                pass

def get_key(stock):
    '''
    获取stock在g.hold_stock_name对应的key值
    '''
    for n in g.hold_stock_name.items():
        if n[1] == stock:
            return n[0]

def get_buy_cash(i):
    '''
    获取g.hold中i键对应仓位的可用购买现金，
    这里设定：将剩余四成仓位分两次购买
    （如需更改购买现金占比，请自行修改）
    并在现金用尽之后，将股票加入“欲卖出”股票列表，如下跌找过一定比例，则进行止损
    '''
    all_cash = g.hold[i]['cash'] #仓位中可用现金
    total = g.hold[i]['total'] #仓位初始总资金
    if all_cash/total > 0.3:
        cash = all_cash*0.5
        return cash
    elif all_cash/total < 0.3:
        will_sell_stock = g.hold_stock_name[i]
        if will_sell_stock not in g.per_sell_stock:
            g.per_sell_stock.append(will_sell_stock)
        cash = all_cash
        return cash

def stop_loss(context):
    '''
    “欲卖出”股票列表，如下跌找过一定比例，则进行止损
    '''
    if len(g.per_sell_stock)>0:
        current_data = get_current_data(g.per_sell_stock)
        for stock in g.per_sell_stock:
            if current_data[stock].paused == 0: #跳过停牌
                avg_cost = context.portfolio.positions[stock].avg_cost
                price = context.portfolio.positions[stock].price
                if (price/avg_cost) <= 0.85:
                    order_target(stock, 0)
                    g.per_sell_stock.remove(stock)
                    log.info("Stoploss: %s, avg_cost=%.2f, price=%.2f", stock, avg_cost, price)
            else:
                pass
    else:
        pass


def change_position_cash(context):
    '''
    I.  更新每个仓位的可用现金

    II. 将已卖掉的股票现金仓位进行资金再平衡。
    （这里只是将当前空余的仓位进行了资金再平衡
    如，有N支仓位空余，及重新平均分配N支仓位的资金）
    '''
    g.selled_list_keys = [] #已卖掉的股票keys
    g.rebalance_total_money = 0 #再分仓的总金额

    trades=get_orders() #过去当天订单
    hold_stock = context.portfolio.positions
    for t in trades.values(): 
        # 更新每个仓位的可用现金
        if t.is_buy and t.filled > 0: #买入有效订单
            if (t.security not in g.already_hold_stock) and (hold_stock[t.security].sellable_amount  > 0):
                i = get_key(t.security)
                # g.hold[i]['initial_buy_cash'] = t.cash
                g.hold[i]['cash'] = g.hold[i]['total'] - t.cash #更新可用现金
            elif (t.security in g.already_hold_stock) and (hold_stock[t.security].sellable_amount  > 0):
                i = get_key(t.security)
                # g.hold[i]['initial_buy_cash'] = g.hold[i]['cash'] + t.cash
                g.hold[i]['cash'] = g.hold[i]['cash'] - t.cash #更新可用现金

        # 将已卖掉的股票现金仓位进行资金再平衡。
        elif not t.is_buy and t.filled > 0:#卖出有效订单
            if t.security not in context.portfolio.positions.keys():
                i = get_key(t.security)
                g.selled_list_keys.append(i)
                g.rebalance_total_money += t.cash

    # 资金重分配函数，如需隔离仓位，则不用执行该函数
    if len(g.selled_list_keys) > 0:
        rebalance_money(g.selled_list_keys, g.rebalance_total_money)
    
    # 打印结果（用于调试）
    # log.info('hold:',g.hold)
    log.info('hold_stock_name:',g.hold_stock_name)
    log.info('per_sell_stock:',g.per_sell_stock)

def rebalance_money(selled_list_keys, rebalance_total_money):
    '''
    资金重分配函数
    如需隔离仓位，则不用执行该函数
    '''
    # 获取再分仓的总金额
    for i in selled_list_keys:
        rebalance_total_money += g.hold[i]['cash']
    
    # 确定每个仓位的总金额
    every_position = rebalance_total_money/len(selled_list_keys)
    
    # 资金重分配
    for i in selled_list_keys:
        g.hold_stock_name[i] = None
        g.hold[i]['total'] = every_position
        g.hold[i]['initial_buy_cash'] = every_position*g.proportion_initial_buy_cash
        g.hold[i]['cash'] = every_position*g.proportion_cash
