# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


from stl_utils.const import EVENT_TYPE
import datetime


'''
轻量级的试验场地2号
'''


class TradeEventSource:
    TRADE_DAY_COUNT = 2       # 天数
    TRADE_MINUTE_COUNT = 242  # 每个交易日的交易分钟数(60*2+1)*2
    TRADE_MINUTE_STEP = 1     # 分钟级事件发送时间间隔
    TRADE_TICK_COUNT = 59     # 每分钟的TICK数
    TRADE_TICK_STEP = 2       # TICK级事件发送时间间隔

    def __init__(self, source_type):
        self.data_list = range(1, TradeEventSource.TRADE_DAY_COUNT+1)
        if source_type == 'DAY':
            self.generator = self.create_day_generator()
        elif source_type == 'MINUTE':
            self.generator = self.create_minute_generator()
        elif source_type == 'TICK':
            self.generator = self.create_tick_generator()

    def create_day_generator(self):
        day = datetime.datetime.today()
        for date in self.data_list:
            yield day.replace(hour=9, minute=0, second=0), EVENT_TYPE.DAY_START
            yield day.replace(hour=15, minute=0, second=0), EVENT_TYPE.HANDLE_BAR
            yield day.replace(hour=16, minute=0, second=0), EVENT_TYPE.DAY_END
            day = day + datetime.timedelta(hours=24)

    def create_minute_generator(self):
        day = datetime.datetime.today()
        for index in self.data_list:
            # 开盘
            yield day.replace(hour=9, minute=0, second=0), EVENT_TYPE.DAY_START
            # 盘间
            for minute in range(0, TradeEventSource.TRADE_MINUTE_COUNT+1, TradeEventSource.TRADE_MINUTE_STEP):
                # 上午
                if minute >= 0 and minute < 30:
                    yield day.replace(hour=9, minute=minute+30, second=0), EVENT_TYPE.HANDLE_BAR
                if minute >= 30 and minute < 90:
                    yield day.replace(hour=10, minute=minute-30, second=0), EVENT_TYPE.HANDLE_BAR
                if minute >= 90 and minute < 120:
                    yield day.replace(hour=11, minute=minute-90, second=0), EVENT_TYPE.HANDLE_BAR
                # 边界值
                if minute == 120:
                    yield day.replace(hour=11, minute=minute-90, second=0), EVENT_TYPE.HANDLE_BAR
                    yield day.replace(hour=13, minute=minute-120, second=0), EVENT_TYPE.HANDLE_BAR
                # 下午
                if minute > 120 and minute < 180:
                    yield day.replace(hour=13, minute=minute-120, second=0), EVENT_TYPE.HANDLE_BAR
                if minute >= 180 and minute < 240:
                    yield day.replace(hour=14, minute=minute-180, second=0), EVENT_TYPE.HANDLE_BAR
                if minute == 240:
                    yield day.replace(hour=15, minute=minute-240, second=0), EVENT_TYPE.HANDLE_BAR
            # 收盘
            yield day.replace(hour=16, minute=0, second=0), EVENT_TYPE.DAY_END
            day = day + datetime.timedelta(hours=24)

    def create_tick_generator(self):
        day = datetime.datetime.today()
        for index in self.data_list:
            # 开盘
            yield day.replace(hour=9, minute=0, second=0), EVENT_TYPE.DAY_START
            # 盘间
            is_morning_trade_over = False
            is_aftrernoon_trade_over = False
            for minute in range(0, TradeEventSource.TRADE_MINUTE_COUNT+1, TradeEventSource.TRADE_MINUTE_STEP):
                for tick in range(0, TradeEventSource.TRADE_TICK_COUNT+1, TradeEventSource.TRADE_TICK_STEP):
                    # 上午
                    if minute >= 0 and minute < 30:
                        yield day.replace(hour=9, minute=minute+30, second=tick), EVENT_TYPE.HANDLE_BAR
                    if minute >= 30 and minute < 90:
                        yield day.replace(hour=10, minute=minute-30, second=tick), EVENT_TYPE.HANDLE_BAR
                    if minute >= 90 and minute < 120:
                        yield day.replace(hour=11, minute=minute-90, second=tick), EVENT_TYPE.HANDLE_BAR
                    # 边界值
                    if minute == 120:
                        if not is_morning_trade_over:
                            yield day.replace(hour=11, minute=minute-90, second=tick), EVENT_TYPE.HANDLE_BAR
                            is_morning_trade_over = True
                        yield day.replace(hour=13, minute=minute-120, second=tick), EVENT_TYPE.HANDLE_BAR
                    # 下午
                    if minute > 120 and minute < 180:
                        yield day.replace(hour=13, minute=minute-120, second=tick), EVENT_TYPE.HANDLE_BAR
                    if minute >= 180 and minute < 240:
                        yield day.replace(hour=14, minute=minute-180, second=tick), EVENT_TYPE.HANDLE_BAR
                    if minute == 240:
                        if not is_aftrernoon_trade_over:
                            yield day.replace(hour=15, minute=minute-240, second=tick), EVENT_TYPE.HANDLE_BAR
                            is_aftrernoon_trade_over = True
            # 收盘
            yield day.replace(hour=16, minute=0, second=0), EVENT_TYPE.DAY_END
            day = day + datetime.timedelta(hours=24)

    def __iter__(self):
        return self

    def __next__(self):
        for date, event in self.generator:
            return date, event
        raise StopIteration


def execute(event_source):
    for day, event in event_source:
        print(day, event)


def event_source_day_test():
    '''
    策略回测功能, 需要日级的事件驱动机制, 在这里做个试验
    '''
    event_source = TradeEventSource('DAY')
    execute(event_source)


def event_source_minute_test():
    '''
    策略回测功能, 需要分钟级的事件驱动机制, 在这里做个试验
    '''
    event_source = TradeEventSource('MINUTE')
    execute(event_source)


def event_source_tick_test():
    '''
    策略回测功能, 需要TICK级的事件驱动机制, 在这里做个试验
    '''
    event_source = TradeEventSource('TICK')
    execute(event_source)



if __name__ == '__main__':
    # event_source_day_test()
    # event_source_minute_test()
    event_source_tick_test()


