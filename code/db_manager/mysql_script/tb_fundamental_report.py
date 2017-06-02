# -*- coding: utf-8 -*-
#
# Copyright 2017 Xilosopher
#
# Author: Moro JoJo


sql_create_tb_fundamental_report = '''
DROP TABLE IF EXISTS tb_fundamental_report;
CREATE TABLE tb_fundamental_report (
    c_code VARCHAR(10) PRIMARY KEY NOT NULL COMMENT 'Security code 代码',
    c_name VARCHAR(20) NOT NULL COMMENT 'Security name 名称',
    c_eps DOUBLE COMMENT 'Security Earn Per Share 每股收益',
    c_eps_yoy DOUBLE COMMENT 'Security Earn Per Share YOY 每股收益同比（%）',
    c_bvps DOUBLE COMMENT 'Security Book Value Per Share (RMB Yuan) 每股净资产',
    c_roe DOUBLE COMMENT 'Security Rate of Return on Common Stockholders’ Equity 净资产收益率（%）',
    c_cfps DOUBLE COMMENT 'Security Cash Flow Per Share 每股现金流',
    c_np DOUBLE  COMMENT 'Security Net Profit (RMB Yuan) 净利润（万元）',
    c_profit_yoy DOUBLE  COMMENT 'Security Net Profit YOY 净利润同比（%）',
    c_distribute VARCHAR(40) COMMENT 'Security distribute 分配方案',
    c_report_date VARCHAR(10) COMMENT 'Security report date 报告发布日期'
)
ENGINE=InnoDB DEFAULT CHARSET=utf8
COMMENT = 'Basic info of all securities';
'''

sql_update_tb_fundamental_report = '''
INSERT INTO tb_fundamental_report (
    c_code, c_name, c_eps, c_eps_yoy, c_bvps, c_roe,
    c_cfps, c_np, c_profit_yoy, c_distribute, c_report_date
)
VALUES (
    '%s', '%s', %lf, %lf, %lf, %lf,
    %lf, %lf, %lf, '%s', '%s'
)'''

