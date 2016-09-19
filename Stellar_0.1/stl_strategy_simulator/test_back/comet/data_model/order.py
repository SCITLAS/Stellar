# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import uuid
import abc

from six import with_metaclass
from stl_utils.const import ORDER_STATUS


'''
交易委托
'''


def gen_order_id():
    return uuid.uuid4().hex


class OrderStyle(with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def get_limit_price(self, is_buy):
        raise NotImplementedError


class MarketOrder(OrderStyle):

    def get_limit_price(self, _is_buy):
        return None


class LimitOrder(OrderStyle):

    def __init__(self, limit_price):
        self.limit_price = limit_price

    def get_limit_price(self, is_buy):
        return self.limit_price


# TODO use nametuple to reduce memory

class Order(object):

    def __init__(self, dt, order_book_id, quantity, style):
        self.dt = dt
        self.order_book_id = order_book_id
        self._style = style
        self._order_id = gen_order_id()

        self.filled_shares = 0.0
        self.quantity = quantity
        self._reject_reason = ""

        self.status = ORDER_STATUS.OPEN

    @property
    def style(self):
        return self._style

    @property
    def order_id(self):
        return self._order_id

    @property
    def instrument(self):
        raise NotImplementedError

    def cancel(self):
        raise NotImplementedError

    def fill(self, shares):
        self.filled_shares += shares

        assert self.filled_shares <= self.quantity

    def mark_rejected(self, reject_reason):
        self._reject_reason = reject_reason
        self.status = ORDER_STATUS.REJECTED

    @property
    def is_buy(self):
        return self.quantity > 0

    @property
    def reject_reason(self):
        return self._reject_reason

    def __repr__(self):
        return "Order({0})".format(self.__dict__)
