# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


from six import with_metaclass

import abc


'''
税金
'''


class BaseTax(with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def get_tax(self, data_proxy, order):
        raise NotImplementedError


class AStockTax(BaseTax):
    def __init__(self, tax_rate=0.001):
        self.tax_rate = tax_rate

    def get_tax(self, order, trade):
        cost_money = trade.price * abs(trade.amount)
        tax = 0
        if trade.amount < 0:
            tax = cost_money * self.tax_rate

        return tax