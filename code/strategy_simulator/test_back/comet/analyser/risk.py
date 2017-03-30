# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


# from __future__ import division

import copy
import numpy as np
from collections import OrderedDict

from utils import const


'''
风险
'''


class Risk(object):
    '''
    风险数据类
    '''
    def __init__(self):
        self.volatility = .0
        self.alpha = .0
        self.beta = .0
        self.sharpe = .0
        self.sortino = .0
        self.information_rate = .0
        self.max_drawdown = .0
        self.tracking_error = .0
        self.downside_risk = .0

    def __repr__(self):
        return "Risk({0})".format(self.__dict__)


class RiskCalculator(object):
    '''
    风险计算类
    '''
    def __init__(self, trading_params, data_proxy):
        self.data_proxy = data_proxy

        self.start_date = trading_params.start_date
        self.trading_index = trading_params.trading_calendar
        self.trading_days_cnt = len(self.trading_index)

        self.simulation_total_daily_returns = np.full(self.trading_days_cnt, np.nan)
        self.benchmark_total_daily_returns = np.full(self.trading_days_cnt, np.nan)
        self.simulation_current_daily_returns = None
        self.benchmark_current_daily_returns = None

        self.simulation_total_returns = np.full(self.trading_days_cnt, np.nan)
        self.benchmark_total_returns = np.full(self.trading_days_cnt, np.nan)
        self.simulation_current_total_returns = None
        self.benchmark_current_total_returns = None

        self.simulation_annualized_returns = np.full(self.trading_days_cnt, np.nan)
        self.benchmark_annualized_returns = np.full(self.trading_days_cnt, np.nan)
        self.simulation_current_annualized_returns = None
        self.benchmark_current_annualized_returns = None

        self.risk = Risk()

        self.daily_risks = OrderedDict()

        self.current_max_returns = -np.inf
        self.current_max_drawdown = 0

        # FIXME might change daily?
        self.risk_free_rate = data_proxy.get_yield_curve(self.trading_index[0], self.trading_index[-1])

    def calculate(self, date, simulation_daily_returns, benchmark_daily_returns):
        idx = self.latest_idx = self.trading_index.get_loc(date)

        # daily
        self.simulation_total_daily_returns[idx] = simulation_daily_returns
        self.benchmark_total_daily_returns[idx] = benchmark_daily_returns
        self.simulation_current_daily_returns = self.simulation_total_daily_returns[:idx + 1]
        self.benchmark_current_daily_returns = self.benchmark_total_daily_returns[:idx + 1]

        self.days_cnt = len(self.simulation_current_daily_returns)
        days_pass_cnt = (date - self.start_date).days + 1

        # risk
        # self.riskfree_total_returns = self.risk_free_rate / self.days_cnt * const.DAYS_CNT.TRADING_DAYS_A_YEAR
        self.riskfree_total_returns = self.risk_free_rate

        # total
        self.simulation_total_returns[idx] = (1. + self.simulation_current_daily_returns).prod() - 1
        self.benchmark_total_returns[idx] = (1. + self.benchmark_current_daily_returns).prod() - 1
        self.simulation_current_total_returns = self.simulation_total_returns[:idx + 1]
        self.benchmark_current_total_returns = self.benchmark_total_returns[:idx + 1]

        # annual
        self.simulation_annualized_returns[idx] = (1 + self.simulation_current_total_returns[-1]) ** (
                    const.DAYS_CNT.DAYS_A_YEAR / days_pass_cnt) - 1
        self.benchmark_annualized_returns[idx] = (1 + self.benchmark_current_total_returns[-1]) ** (
            const.DAYS_CNT.DAYS_A_YEAR / days_pass_cnt) - 1
        self.simulation_current_annualized_returns = self.simulation_annualized_returns[:idx + 1]
        self.benchmark_current_annualized_returns = self.benchmark_annualized_returns[:idx + 1]

        if self.simulation_current_total_returns[-1] > self.current_max_returns:
            self.current_max_returns = self.simulation_current_total_returns[-1]

        risk = self.risk
        risk.volatility = self.cal_volatility()
        risk.max_drawdown = self.cal_max_drawdown()
        risk.tracking_error = self.cal_tracking_error()
        risk.information_rate = self.cal_information_rate(risk.volatility)
        risk.downside_risk = self.cal_downside_risk()
        risk.beta = self.cal_beta()
        risk.alpha = self.cal_alpha()
        risk.sharpe = self.cal_sharpe()
        risk.sortino = self.cal_sortino()

        self.daily_risks[date] = copy.deepcopy(risk)

    def cal_volatility(self):
        daily_returns = self.simulation_current_daily_returns
        if len(daily_returns) <= 1:
            return 0.
        volatility = const.DAYS_CNT.TRADING_DAYS_A_YEAR ** 0.5 * np.std(daily_returns, ddof=1)
        return volatility

    def cal_max_drawdown(self):
        today_return = self.simulation_current_total_returns[-1]
        today_drawdown = (1. + today_return) / (1. + self.current_max_returns) - 1.
        if today_drawdown < self.current_max_drawdown:
            self.current_max_drawdown = today_drawdown
        return self.current_max_drawdown

    def cal_tracking_error(self):
        diff = self.simulation_current_daily_returns - self.benchmark_current_daily_returns
        return ((diff * diff).sum() / len(diff)) ** 0.5 * const.DAYS_CNT.TRADING_DAYS_A_YEAR ** 0.5

    def cal_information_rate(self, volatility):
        simulation_rets = self.simulation_current_daily_returns.sum() / len(self.simulation_current_daily_returns) * const.DAYS_CNT.TRADING_DAYS_A_YEAR
        benchmark_rets = self.benchmark_current_daily_returns.sum() / len(self.benchmark_current_daily_returns) * const.DAYS_CNT.TRADING_DAYS_A_YEAR

        return (simulation_rets - benchmark_rets) / volatility

    def cal_alpha(self):
        beta = self.risk.beta

        simulation_rets = self.simulation_current_daily_returns.sum() / len(self.simulation_current_daily_returns) * const.DAYS_CNT.TRADING_DAYS_A_YEAR
        benchmark_rets = self.benchmark_current_daily_returns.sum() / len(self.benchmark_current_daily_returns) * const.DAYS_CNT.TRADING_DAYS_A_YEAR

        alpha = simulation_rets - (self.riskfree_total_returns + beta * (benchmark_rets - self.riskfree_total_returns))
        return alpha

    def cal_beta(self):
        if len(self.simulation_current_daily_returns) <= 1:
            return 0.
        cov = np.cov(np.vstack([
            self.simulation_current_daily_returns,
            self.benchmark_current_daily_returns,
        ]), ddof=1)
        beta = cov[0][1] / cov[1][1]

        return beta

    def cal_sharpe(self):
        volatility = self.risk.volatility
        simulation_rets = self.simulation_current_daily_returns.sum() / len(self.simulation_current_daily_returns) * const.DAYS_CNT.TRADING_DAYS_A_YEAR

        sharpe = (simulation_rets - self.riskfree_total_returns) / volatility

        return sharpe

    def cal_sortino(self):
        simulation_rets = self.simulation_current_daily_returns.sum() / len(self.simulation_current_daily_returns) * const.DAYS_CNT.TRADING_DAYS_A_YEAR
        downside_risk = self.risk.downside_risk

        sortino = (simulation_rets - self.riskfree_total_returns) / downside_risk
        return sortino

    def cal_downside_risk(self):
        mask = self.simulation_current_daily_returns < self.benchmark_current_daily_returns
        diff = self.simulation_current_daily_returns[mask] - self.benchmark_current_daily_returns[mask]
        if len(diff) <= 1:
            return 0.

        return ((diff * diff).sum() / len(diff)) ** 0.5 * const.DAYS_CNT.TRADING_DAYS_A_YEAR ** 0.5

    def __repr__(self):
        return "RiskCalculator({0})".format(self.__dict__)
