# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


from six import with_metaclass

import abc


'''
佣金
'''


class BaseCommission(with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def get_commission(self, order, trade):
        """get commission

        :param order:
        :param trade:
        :returns: commission for current trade
        :rtype: float
        """
        raise NotImplementedError


class AStockCommission(BaseCommission):
    def __init__(self, commission_rate=0.0008, min_commission=5):
        self.commission_rate = commission_rate
        self.min_commission = min_commission

    def get_commission(self, order, trade):
        cost_money = trade.price * abs(trade.amount)
        v = cost_money * self.commission_rate
        v = max(self.min_commission, v)

        return v