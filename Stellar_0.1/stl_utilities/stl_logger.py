# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


'''
日志工具模块
'''


import logging
import time


def dm_logger(name):
    '''
    获取以name命名的数据管理包日志器

    Parameters
    ------
        name: 记录日志的名称
    return
    -------
        logger: 带名字的logger
    '''

    now = time.strftime('%Y-%m-%d %H:%M:%S')

    logging.basicConfig(
        level = logging.DEBUG,
        format = now + ' : ' + name + ' LINE %(lineno)-4d  %(levelname)-8s %(message)s',
        datefmt = '%m-%d %H:%M',
        filename = '../logs/data_manager_log.log',
        filemode = 'a'
    )

    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(name + ': LINE %(lineno)-4d : %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.addHandler(handler)

    return logger


if __name__ == '__main__':
    dm_logger(__file__).debug('data_manager_logger test')