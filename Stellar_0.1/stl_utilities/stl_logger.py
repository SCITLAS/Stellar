# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


'''
日志工具模块
'''


import logging


DM_LOG_FILE = '../logs/data_manager_log.log'
LOG_LEVEL = logging.DEBUG
class StlDmLogger(logging.Logger):
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


if __name__ == '__main__':
    StlDmLogger().debug('debug')
    StlDmLogger().info('info')
    StlDmLogger().warning('warning')
    StlDmLogger().error('error')
    StlDmLogger().fatal('fatal')
    StlDmLogger().critical('critical')

