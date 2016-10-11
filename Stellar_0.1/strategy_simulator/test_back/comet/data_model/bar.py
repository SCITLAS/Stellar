# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import datetime


'''
Bar相关的数据模型
'''


class BarObject(object):
    def __init__(self, instrument, data):
        self._data = data
        self._instrument = instrument

    @property
    def open(self):
        return self._data["open"]

    @property
    def close(self):
        return self._data["close"]

    @property
    def low(self):
        return self._data["low"]

    @property
    def high(self):
        return self._data["high"]

    @property
    def last(self):
        return self.close

    @property
    def volume(self):
        return self._data["volume"]

    @property
    def datetime(self):
        return datetime.datetime.strptime(str(self._data["date"]), "%Y%m%d%H%M%S")

    @property
    def instrument(self):
        return self._instrument

    @property
    def order_book_id(self):
        return self._instrument.order_book_id

    @property
    def symbol(self):
        return self._instrument.symbol

    @property
    def is_trading(self):
        return self.volume > 0

    def mavg(self, intervals, frequency="day"):
        """
        Returns moving average price for the given security for a give number
            of intervals for a frequency, by default to `"day"`.
        :param int intervals: a given number of intervals, e.g. given number
            of days
        :param str frequency: frequency of the give number of intervals, by
            default as ‘day’.
        """
        raise NotImplementedError

    def vwap(self, intervals, frequency="day"):
        raise NotImplementedError

    def history(self, bar_count, frequency, field):
        raise NotImplementedError

    def __repr__(self):
        return "BarObject({0})".format(self.__dict__)

    def __getitem__(self, key):
        return self.__dict__[key]


class BarMap(object):
    def __init__(self, dt, universe, data_proxy):
        self.dt = dt
        self.universe = universe
        self.data_proxy = data_proxy

    def update_dt(self, dt):
        self.dt = dt

    def __getitem__(self, key):
        return self.data_proxy.get_bar(key, self.dt)

    def __repr__(self):
        return "{0}()".format(type(self).__name__)
