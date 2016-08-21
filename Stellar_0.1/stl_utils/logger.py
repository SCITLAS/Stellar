# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import logging


'''
日志工具模块
'''


DM_LOG_FILE = '../../logs/data_manager_log.log'
LOG_LEVEL = logging.DEBUG


def dm_logger():
    '''
    获取数据管理模块的日志类实例

    Parameters
    ------
        无
    return
    -------
        logger: logging类实例
    '''
    logger = logging.getLogger(name='STL_DM')
    logger.setLevel(level=LOG_LEVEL)

    formatter = logging.Formatter("[%(asctime)s] %(name)s [%(levelname)s] %(filename)s LINE %(lineno)d:  %(message)s")
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(DM_LOG_FILE)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


if __name__ == '__main__':
    dm_logger().debug('debug')
    dm_logger().info('info')
    dm_logger().warning('warning')
    dm_logger().error('error')
    dm_logger().fatal('fatal')
    dm_logger().critical('critical')

