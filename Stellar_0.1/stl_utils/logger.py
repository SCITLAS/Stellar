# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import logging
import sys
import os

from logbook import Logger, StreamHandler
from logbook.more import ColorizedStderrHandler


'''
日志工具模块
'''


__all__ = [
    "dm_log",
]


DATA_MGR_LOG = 0
TEST_BACK_LOG = 1

DM_LOG_FILE = '../../logs/data_manager_log.log'
LOG_LEVEL = logging.DEBUG


def singleton(cls, *args, **kw):
    instances = {}
    def _singleton():
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]
    return _singleton

@singleton
class DmLogger(logging.Logger):
    '''
    data_manager的日志类
    '''
    def __init__(self):
        logging.Logger.__init__(self, name='STL_DM', level=LOG_LEVEL)

        formatter = logging.Formatter("[%(asctime)s] %(name)s [%(levelname)s] %(filename)s LINE %(lineno)d:  %(message)s")
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        self.addHandler(stream_handler)

        file_handler = logging.FileHandler(DM_LOG_FILE)
        file_handler.setFormatter(formatter)
        self.addHandler(file_handler)



def user_log_formatter(record, handler):
    return "{level}: {msg}".format(
        level=record.level_name,
        msg=record.message,
    )


handler = ColorizedStderrHandler()
# handler.formatter = user_log_formatter
handler.push_application()
dm_log2 = Logger(name='DATA_MGR')

dm_log = DmLogger()


if __name__ == '__main__':
    dm_log.debug('debug')
    dm_log.info('info')
    dm_log.warning('warning')
    dm_log.error('error')
    dm_log.fatal('fatal')
    dm_log.critical('critical')

    a = DmLogger()
    b = DmLogger()
    print(id(a))
    print(id(b))
    a.debug('xxx')
    b.debug('yyy')

