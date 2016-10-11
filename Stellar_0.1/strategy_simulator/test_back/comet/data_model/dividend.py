# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


'''
股息
'''


class Dividend(object):
    def __init__(self, order_book_id, quantity, dividend_series):
        self.order_book_id = order_book_id
        self.quantity = quantity
        self.dividend_series = dividend_series