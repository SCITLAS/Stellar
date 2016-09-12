# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


from enum import Enum


'''
常量定义
'''


ORDER_STATUS = Enum("ORDER_STATUS", [
    "OPEN",
    "FILLED",
    "REJECTED",
    "CANCELLED",
])


EVENT_TYPE = Enum("EVENT_TYPE", [
    "DAY_START",
    "HANDLE_BAR",
    "DAY_END",
])


EXECUTION_PHASE = Enum("EXECUTION_PHASE", [
    "INIT",
    "HANDLE_BAR",
    "BEFORE_TRADING",
    "SCHEDULED",
    "FINALIZED",
])


class DAYS_CNT(object):
    DAYS_A_YEAR = 365
    TRADING_DAYS_A_YEAR = 252
