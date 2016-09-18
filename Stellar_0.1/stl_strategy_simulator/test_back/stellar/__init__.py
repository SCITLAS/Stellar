# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'



# from __future__ import division

import datetime
from six import iteritems

from .simulator import trade_executor, trade_executor
from .simulator.trade_context import ExecutionContext


def memoize(function):
    memo = {}
    function.__memo__ = memo

    def wrapper(*args, **kwargs):
        key = "#".join([str(arg) for arg in args] + ["%s:%s" % (k, v) for k, v in iteritems(kwargs)])
        if key in memo:
            return memo[key]
        else:
            rv = function(*args, **kwargs)
            memo[key] = rv
            return rv

    return wrapper


def dummy_func(*args, **kwargs):
    return None


def get_last_date(trading_calendar, dt):
    idx = trading_calendar.searchsorted(dt)
    return trading_calendar[idx - 1]


def convert_date_to_int(dt):
    t = dt.year * 10000 + dt.month * 100 + dt.day
    t *= 1000000
    return t


def convert_dt_to_int(dt):
    t = convert_date_to_int(dt)
    t += dt.hour * 10000 + dt.minute * 100 + dt.second
    return t


def convert_int_to_date(dt_int):
    dt_int = int(dt_int)
    year = dt_int // 10000000000
    month = (dt_int // 100000000) % 100
    day = (dt_int // 1000000) % 100
    return datetime.datetime(year, month, day)

