# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import click
from six import iteritems
import pandas as pd

from ..data_model.bar import BarMap
from ..utils import ExecutionContext, dummy_func
from .scheduler import scheduler

from ..analyser.trade_simulation import TradeSimulation
from .trade_context import TradeSimulationContext
from .event import TradeSimulationEventSource

from stl_utils.const import EVENT_TYPE, EXECUTION_PHASE


'''
策略模拟交易执行器
'''


class TradeSimulationExecutor(object):
    def __init__(self, trading_params, data_proxy, **kwargs):
        """init

        :param Strategy strategy: current user strategy object
        :param TradingParams trading_params: current trading params
        :param DataProxy data_proxy: current data proxy to access data
        """
        self.trading_params = trading_params
        self._data_proxy = data_proxy

        self._trade_simulation_context = kwargs.get("trade_simulation_context")
        if self._trade_simulation_context is None:
            self._trade_simulation_context = TradeSimulationContext()

        self._user_init = kwargs.get("init", dummy_func)
        self._user_handle_bar = kwargs.get("handle_bar", dummy_func)
        self._user_before_trading = kwargs.get("before_trading", dummy_func)

        self._trade_simulation = kwargs.get("trade_simulation")
        if self._trade_simulation is None:
            self._trade_simulation = TradeSimulation(data_proxy, trading_params)

        self._event_source = TradeSimulationEventSource(trading_params)
        self._current_dt = None
        self.current_universe = set()

        self.progress_bar = click.progressbar(length=len(self.trading_params.trading_calendar), show_eta=False)

    def execute(self):
        """run strategy

        :returns: performance results
        :rtype: pandas.DataFrame

        """
        # use local variable for performance
        data_proxy = self.data_proxy
        trade_simulation_context = self.trade_simulation_context
        trade_simulation = self.trade_simulation

        init = self._user_init
        before_trading = self._user_before_trading
        handle_bar = self._user_handle_bar

        trade_simulation_on_dt_change = trade_simulation.on_dt_change
        trade_simulation_on_bar_close = trade_simulation.on_bar_close
        trade_simulation_on_day_open = trade_simulation.on_day_open
        trade_simulation_on_day_close = trade_simulation.on_day_close
        trade_simulation_update_portfolio = trade_simulation.update_portfolio

        is_show_progress_bar = self.trading_params.show_progress

        def on_dt_change(dt):
            self._current_dt = dt
            trade_simulation_on_dt_change(dt)

        with ExecutionContext(self, EXECUTION_PHASE.INIT):
            init(trade_simulation_context)

        try:
            for dt, event in self._event_source:
                on_dt_change(dt)

                bar_dict = BarMap(dt, self.current_universe, data_proxy)

                if event == EVENT_TYPE.DAY_START:
                    with ExecutionContext(self, EXECUTION_PHASE.BEFORE_TRADING, bar_dict):
                        trade_simulation_on_day_open()
                        before_trading(trade_simulation_context, None)

                elif event == EVENT_TYPE.HANDLE_BAR:
                    with ExecutionContext(self, EXECUTION_PHASE.HANDLE_BAR, bar_dict):
                        trade_simulation_update_portfolio(bar_dict)
                        handle_bar(trade_simulation_context, bar_dict)
                        scheduler.next_day(dt, trade_simulation_context, bar_dict)
                        trade_simulation_on_bar_close(bar_dict)

                elif event == EVENT_TYPE.DAY_END:
                    with ExecutionContext(self, EXECUTION_PHASE.FINALIZED, bar_dict):
                        trade_simulation_on_day_close()

                    if is_show_progress_bar:
                        self.progress_bar.update(1)
        finally:
            self.progress_bar.render_finish()

        results_df = self.generate_result(trade_simulation)

        return results_df

    def generate_result(self, trade_simulation):
        """generate result dataframe

        :param trade_simulation:
        :returns: result dataframe contains daliy portfolio, risk and trades
        :rtype: pd.DataFrame
        """
        account = trade_simulation.account
        risk_cal = trade_simulation.risk_cal
        columns = [
            "daily_returns",
            "total_returns",
            "annualized_returns",
            "market_value",
            "portfolio_value",
            "total_commission",
            "total_tax",
            "pnl",
            "positions",
            "cash",
        ]
        risk_keys = [
            "volatility", "max_drawdown",
            "alpha", "beta", "sharpe",
            "information_rate", "downside_risk",
            "tracking_error", "sortino",
        ]

        data = []
        for date, portfolio in iteritems(trade_simulation.daily_portfolios):
            # portfolio
            items = {"date": pd.Timestamp(date)}
            for key in columns:
                items[key] = getattr(portfolio, key)

            # trades
            items["trades"] = account.get_all_trades()[date]

            # risk
            risk = risk_cal.daily_risks[date]
            for risk_key in risk_keys:
                items[risk_key] = getattr(risk, risk_key)

            idx = risk_cal.trading_index.get_loc(date)
            items["benchmark_total_returns"] = risk_cal.benchmark_total_returns[idx]
            items["benchmark_daily_returns"] = risk_cal.benchmark_total_daily_returns[idx]
            items["benchmark_annualized_returns"] = risk_cal.benchmark_annualized_returns[idx]

            data.append(items)

        results_df = pd.DataFrame(data)
        results_df.set_index("date", inplace=True)

        return results_df

    @property
    def trade_simulation_context(self):
        """get current trade simulation context

        :returns: current trade simulation context
        :rtype: TradeSimulationContext
        """
        return self._trade_simulation_context

    @property
    def trade_simulation(self):
        """get current trade simulation

        :returns: current trade simulation
        :rtype: TradeSimulation
        """
        return self._trade_simulation

    @property
    def data_proxy(self):
        """get current data proxy

        :returns: current data proxy
        :rtype: DataProxy
        """
        return self._data_proxy

    @property
    def current_dt(self):
        """get current simulation datetime

        :returns: current datetime
        :rtype: datetime.datetime
        """
        return self._current_dt

