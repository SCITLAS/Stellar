# -*- coding: utf-8 -*-
#
# Copyright 2017 Xilosopher
#
# Author: Moro JoJo



sql_create_tb_fundamental_basic_info = '''
DROP TABLE IF EXISTS tb_fundamental_basic_info;
CREATE TABLE tb_fundamental_basic_info (
    c_code VARCHAR(10) PRIMARY KEY NOT NULL COMMENT 'Security code 代码',
    c_name VARCHAR(20) NOT NULL COMMENT 'Security name 名称',
    c_industry VARCHAR(40) COMMENT 'Security industry 领域',
    c_area VARCHAR(20) COMMENT 'Security area 区域',
    c_pe DOUBLE COMMENT 'Security PE 市盈率',
    c_circulating_cap DOUBLE COMMENT 'Security circulating capitalization, (100 million RMB Yuan) 流通股本',
    c_total_cap DOUBLE COMMENT 'Security total capitalization (100 million RMB Yuan) 总股本',
    c_total_assets DOUBLE COMMENT 'Security total assets (10 thousand RMB Yuan) 总资产',
    c_liquid_assets DOUBLE COMMENT 'Security liquid assets (10 thousand RMB Yuan) 流动资产',
    c_fixed_assets DOUBLE  COMMENT 'Security fixed assets (10 thousand RMB Yuan) 固定资产',
    c_reserved DOUBLE COMMENT 'Security reserved fund (10 thousand RMB Yuan) 公积金',
    c_rps DOUBLE COMMENT 'Security reserved fund per share (RMB Yuan) 每股公积金',
    c_eps VARCHAR(20) COMMENT 'Security Earning Per Share (RMB Yuan) 每股收益',
    c_bvps DOUBLE COMMENT 'Security Book Value Per Share (RMB Yuan) 每股净资产',
    c_pb DOUBLE  COMMENT 'Security Price To Book Ratio 市净率',
    c_ipo_date BIGINT COMMENT 'Security IPO date 上市日期',
    c_up DOUBLE COMMENT 'Security undistributed profit 未分配利润',
    c_upps DOUBLE COMMENT 'Security undistributed profit per share(RMB Yuan) 每股未分配利润',
    c_revenue_yoy DOUBLE COMMENT 'Security revenue increase rate YOY 收入同比',
    c_profit_yoy DOUBLE COMMENT 'Security profit increase rate YOY 利润同比',
    c_gpr DOUBLE COMMENT 'Security gross profit rate 毛利率',
    c_npr DOUBLE COMMENT 'Security net profit rate 净利润率',
    c_shareholders BIGINT COMMENT 'Number of shareholders 股东人数'
)
ENGINE=InnoDB DEFAULT CHARSET=utf8
COMMENT = 'Basic info of all securities';
'''

sql_update_tb_fundamental_basic_info = '''
INSERT INTO tb_fundamental_basic_info (
    c_code, c_name, c_industry, c_area, c_pe, c_circulating_cap, c_total_cap, c_total_assets,
    c_liquid_assets, c_fixed_assets, c_reserved, c_rps, c_eps, c_bvps, c_pb, c_ipo_date,
    c_up, c_upps, c_revenue_yoy, c_profit_yoy, c_gpr, c_npr, c_shareholders
)
VALUES (
    '%s', '%s', '%s', '%s', %lf, %lf, %lf, %lf,
    %lf, %lf, %lf, %lf, '%s', %lf, %lf, %d,
    %lf, %lf, %lf, %lf, %lf, %lf, %d
)'''
