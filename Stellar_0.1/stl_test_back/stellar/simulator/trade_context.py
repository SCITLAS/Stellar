# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import copy
import six

from ..const import EXECUTION_PHASE
from ..utils import ExecutionContext
from ..analyser.commission import AStockCommission
from ..analyser.slippage import FixedPercentSlippageDecider


'''
策略模拟交易上下文
'''


class TradeSimulationContext(object):
    def __init__(self):
        self.__last_portfolio_update_dt = None

    @property
    def now(self):
        return ExecutionContext.get_current_dt()

    @property
    def slippage(self):
        return copy.deepcopy(ExecutionContext.get_trade_simulation().account.slippage_decider)

    @slippage.setter
    @ExecutionContext.enforce_phase(EXECUTION_PHASE.INIT)
    def slippage(self, value):
        assert isinstance(value, (int, float))

        ExecutionContext.get_trade_simulation().account.slippage_decider = FixedPercentSlippageDecider(rate=value)

    @property
    def commission(self):
        return copy.deepcopy(ExecutionContext.get_trade_simulation().account.commission_decider)

    @commission.setter
    @ExecutionContext.enforce_phase(EXECUTION_PHASE.INIT)
    def commission(self, value):
        assert isinstance(value, (int, float))

        ExecutionContext.get_trade_simulation().account.commission_decider = AStockCommission(commission_rate=value)

    @property
    def benchmark(self):
        return copy.deepcopy(ExecutionContext.get_trading_params().benchmark)

    @benchmark.setter
    @ExecutionContext.enforce_phase(EXECUTION_PHASE.INIT)
    def benchmark(self, value):
        assert isinstance(value, six.string_types)

        ExecutionContext.get_trading_params().benchmark = value

    @property
    def short_selling_allowed(self):
        raise NotImplementedError

    @short_selling_allowed.setter
    def short_selling_allowed(self):
        raise NotImplementedError

    @property
    def portfolio(self):
        dt = self.now
        # if self.__last_portfolio_update_dt != dt:
        # FIXME need to use cache, or might use proxy rather then copy
        if True:
            self.__portfolio = copy.deepcopy(ExecutionContext.get_trade_simulation().account.portfolio)
            self.__last_portfolio_update_dt = dt
        return self.__portfolio

    def __repr__(self):
        items = ("%s = %r" % (k, v)
                 for k, v in self.__dict__.items()
                 if not callable(v) and not k.startswith("_"))
        return "Context({%s})" % (', '.join(items), )
