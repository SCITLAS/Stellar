def initialize(context):
    # 定义一个全局变量, 保存要操作的股票
 	# 选股
	g.count = 20
#	run_monthly(update_stock_set,1,time = 'before_open')

#def update_stock_set(context):
	g.stock = get_industry_stocks('C39')+get_industry_stocks('I64')\
		+get_industry_stocks('I65')+get_industry_stocks('R85')\
		+get_industry_stocks('R86')
	#选fundamental比较好的前15只,pe_ration<30，market_cap，按market_cap取后15只股票。
	q = query(\
		valuation.code, valuation.market_cap, valuation.pe_ratio\
	).filter(\
		valuation.code.in_(g.stock),
		valuation.pe_ratio < 20\
	).order_by(\
		valuation.market_cap.asc()\
	).limit(\
		g.count\
	)
	df = get_fundamentals(q)
	g.stock = list(df['code'])
	set_universe(list(df['code']))
	g.cash = 1000000
# 每个单位时间(如果按天回测,则每天调用一次,如果按分钟,则每分钟调用一次)调用一次
# 择时
def handle_data(context, data):
    # if context.portfolio.positions.keys() !=[]:
    #     for stock in context.portfolio.positions.keys():
    #         if stock not in g.stock:
    #             order_target_value(stock, 0)
    # 指数止损
    hist1 = attribute_history('000001.XSHG', 2, '1d', 'close',df=False)
    index_return = (hist1['close'][-1]-hist1['close'][0])/hist1['close'][0]
    print hist1,index_return
    if  index_return<= -0.03:
        for stock in g.stock:
            order_target_value(stock,0)
            log.info("Sell %s for stoploss" %stock)
        return
    for stock in g.stock:
    	his = history(6,'1d','close',[stock],df =False)
    	cnt = 0
    	for i in range(len(his[stock])-1):
    		daily_returns = (his[stock][i+1] - his[stock][i])/his[stock][i]
    		if daily_returns <0:
    			cnt += 1
    	if cnt == 5:
    		return
    	#2.大于5日平均或10日平均20%以上
    	current_price = data[stock].price
    	mavg5 = data[stock].mavg(5)
    	print mavg5
    	mavg10 = data[stock].mavg(10)
    	if current_price > 1.2*mavg5 or current_price > 1.2*mavg10:
    		return
    	#建仓，5步法、将头寸5等分，每下跌2%加一部分
    	cash = context.portfolio.cash
    	amount = int(g.cash/g.count*2/current_price/300)
    	returns = data[stock].returns
    	if returns > 0.01 and context.portfolio.positions[stock].amount < 300*amount and cash > 0:
    		order_value(stock,g.cash/g.count/5)
    		log.info("Buying %s"%stock)
    	#跌10%的严格止损, 或者整体收益大于10%（止盈）
    	cost = context.portfolio.positions[stock].avg_cost
    	if cost != 0:
    	    security_returns = (current_price-cost)/cost
    	    if security_returns < -0.05 or security_returns > 0.15:
    	        order_target_value(stock,0)
    	        log.info("Selling %s" % stock)
		#根据大盘止损
		
		#15分钟K线，连续4次收益率为正且收益率大于8%(暂不做分钟级）
#还未实现复利滚动