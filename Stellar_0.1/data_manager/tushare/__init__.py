# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'





'''


CSV文件存储结构:

# 基本面数据
../../../Data/csv/tushare/security_fundamental_data/basic.csv
../../../Data/csv/tushare/security_fundamental_data/report/2016Q1.csv
../../../Data/csv/tushare/security_fundamental_data/profit/2016Q1.csv
../../../Data/csv/tushare/security_fundamental_data/operation/2016Q1.csv
../../../Data/csv/tushare/security_fundamental_data/growth/2016Q1.csv
../../../Data/csv/tushare/security_fundamental_data/debt_pay/2016Q1.csv
../../../Data/csv/tushare/security_fundamental_data/cashflow/2016Q1.csv

# 指数历史行情数据
../../../Data/csv/tushare/index_trade_data/all/000001.csv
../../../Data/csv/tushare/index_trade_data/trade/recent/day/000001.csv
../../../Data/csv/tushare/index_trade_data/trade/recent/week/000001.csv
../../../Data/csv/tushare/index_trade_data/trade/recent/month/000001.csv
../../../Data/csv/tushare/index_trade_data/trade/recent/5min/000001.csv
../../../Data/csv/tushare/index_trade_data/trade/recent/15min/000001.csv
../../../Data/csv/tushare/index_trade_data/trade/recent/30min/000001.csv
../../../Data/csv/tushare/index_trade_data/trade/recent/60min/000001.csv

# 个股历史行情数据
../../../Data/csv/tushare/security_trade_data/trade/all/002612.csv
../../../Data/csv/tushare/security_trade_data/trade/recent/day/002612.csv
../../../Data/csv/tushare/security_trade_data/trade/recent/week/002612.csv
../../../Data/csv/tushare/security_trade_data/trade/recent/month/002612.csv
../../../Data/csv/tushare/security_trade_data/trade/recent/5min/002612.csv
../../../Data/csv/tushare/security_trade_data/trade/recent/15min/002612.csv
../../../Data/csv/tushare/security_trade_data/trade/recent/30min/002612.csv
../../../Data/csv/tushare/security_trade_data/trade/recent/60min/002612.csv
../../../Data/csv/tushare/security_trade_data/trade/current_day/trade.csv

# TICK数据
../../../Data/csv/tushare/security_trade_data/tick/history/2016-08-15/002612.csv
../../../Data/csv/tushare/security_trade_data/tick/current_day/002612.csv

# 大单数据
../../../Data/csv/tushare/security_trade_data/big_deal/history/2016-08-15/vol_500.csv
../../../Data/csv/tushare/security_trade_data/big_deal/current_day/vol_500.csv

# 股票分类数据
../../../Data/csv/tushare/shibor_data/security_classify_data/industry.csv
../../../Data/csv/tushare/shibor_data/security_classify_data/concept.csv
../../../Data/csv/tushare/shibor_data/security_classify_data/area.csv
../../../Data/csv/tushare/shibor_data/security_classify_data/sme.csv
../../../Data/csv/tushare/shibor_data/security_classify_data/gem.csv
../../../Data/csv/tushare/shibor_data/security_classify_data/st.csv
../../../Data/csv/tushare/shibor_data/security_classify_data/hs300.csv
../../../Data/csv/tushare/shibor_data/security_classify_data/sz50.csv
../../../Data/csv/tushare/shibor_data/security_classify_data/zz500.csv
../../../Data/csv/tushare/shibor_data/security_classify_data/terminated.csv
../../../Data/csv/tushare/shibor_data/security_classify_data/suspended.csv

# 股票参考数据
../../../Data/csv/tushare/security_reference_data/profit_2016.csv
../../../Data/csv/tushare/security_reference_data/forecast_2016Q1.csv
../../../Data/csv/tushare/security_reference_data/restricted_share_2016_1.csv
../../../Data/csv/tushare/security_reference_data/fund_holding_2016Q1.csv
../../../Data/csv/tushare/security_reference_data/new_security.csv
../../../Data/csv/tushare/security_reference_data/sh_margin_2015-01-01_2016-01-01.csv
../../../Data/csv/tushare/security_reference_data/sh_margin_detail_2015-01-01_2016-01-01.csv
../../../Data/csv/tushare/security_reference_data/sz_margin_2015-01-01_2016-01-01.csv
../../../Data/csv/tushare/security_reference_data/sz_margin_detail_2015-01-01_2016-01-01.csv

# 龙虎榜数据
../../../Data/csv/tushare/security_rank_data/top_list/2016-08-12.csv
../../../Data/csv/tushare/security_rank_data/top_statistics/10days.csv
../../../Data/csv/tushare/security_rank_data/broker/top_10days.csv
../../../Data/csv/tushare/security_rank_data/institution/top_10days.csv
../../../Data/csv/tushare/security_rank_data/institution/detail.csv

# 银行间拆借数据
../../../Data/csv/tushare/shibor_data/shibor/2016.csv
../../../Data/csv/tushare/shibor_data/shibor_quote/2016.csv
../../../Data/csv/tushare/shibor_data/shibor_ma/2016.csv
../../../Data/csv/tushare/shibor_data/lpr/2016.csv
../../../Data/csv/tushare/shibor_data/lpr_ma/2016.csv

# 社交媒体和新闻数据
../../../Data/csv/tushare/social_news_data/news/latest_100.csv
../../../Data/csv/tushare/social_news_data/notice/002612_2016-08-15.csv
../../../Data/csv/tushare/social_news_data/sina_guba/data.csv

# 宏观经济数据
../../../Data/csv/tushare/macro_economy_data/deposit_rate.csv
../../../Data/csv/tushare/macro_economy_data/loan_rate.csv
../../../Data/csv/tushare/macro_economy_data/rrr.csv
../../../Data/csv/tushare/macro_economy_data/money_supply.csv
../../../Data/csv/tushare/macro_economy_data/money_supply_bal.csv
../../../Data/csv/tushare/macro_economy_data/gdp_year.csv
../../../Data/csv/tushare/macro_economy_data/gdp_quarter.csv
../../../Data/csv/tushare/macro_economy_data/gdp_for.csv
../../../Data/csv/tushare/macro_economy_data/gdp_pull.csv
../../../Data/csv/tushare/macro_economy_data/gdp_contribution.csv
../../../Data/csv/tushare/macro_economy_data/cpi.csv
../../../Data/csv/tushare/macro_economy_data/ppi.csv


'''
