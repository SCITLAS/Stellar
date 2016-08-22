# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import logging
import sys

from logbook import Logger, StreamHandler
from logbook.more import ColorizedStderrHandler


'''
日志工具模块
'''


DM_LOG_FILE = '../../logs/data_manager_log.log'
LOG_LEVEL = logging.DEBUG


# def dm_logger():
#     '''
#     获取数据管理模块的日志类实例
#
#     Parameters
#     ------
#         无
#     return
#     -------
#         logger: logging类实例
#     '''
#     logger = logging.getLogger(name='STL_DM')
#     logger.setLevel(level=LOG_LEVEL)
#
#     formatter = logging.Formatter("[%(asctime)s] %(name)s [%(levelname)s] %(filename)s LINE %(lineno)d:  %(message)s")
#     stream_handler = logging.StreamHandler()
#     stream_handler.setFormatter(formatter)
#     logger.addHandler(stream_handler)
#
#     file_handler = logging.FileHandler(DM_LOG_FILE)
#     file_handler.setFormatter(formatter)
#     logger.addHandler(file_handler)
#
#     return logger


# def user_log_formatter(record, handler):
#     return "{level}: {msg}".format(
#         level=record.level_name,
#         msg=record.message,
#     )


# handler = StreamHandler(sys.stdout)
handler = ColorizedStderrHandler()
# handler.formatter = user_log_formatter
handler.push_application()

StreamHandler(sys.stdout).push_application()
dm_log = Logger(name='dm_log')



def user_print(*args, **kwargs):
    sep = kwargs.get("sep", " ")
    end = kwargs.get("end", "")

    message = sep.join(map(str, args)) + end

    dm_log.info(message)


if __name__ == '__main__':
    dm_log.debug('debug')
    dm_log.info('info')
    dm_log.notice('notice')
    dm_log.warning('warning')
    dm_log.error('error')
    dm_log.critical('critical')
