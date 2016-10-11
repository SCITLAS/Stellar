# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import logging
from utils.common import get_project_root


'''
日志工具模块
'''


__all__ = ['dm_log']


DATA_MGR_LOG = 0
TEST_BACK_LOG = 1

DM_LOG_FILE = '%s/logs/data_manager_log.log' % get_project_root()
BT_LOG_FILE = '%s/logs/back_test_log.log' % get_project_root()

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

dm_log = DmLogger()


@singleton
class BTLogger(logging.Logger):
    '''
    data_manager的日志类
    '''
    def __init__(self):
        logging.Logger.__init__(self, name='STL_BACK_TEST', level=LOG_LEVEL)

        formatter = logging.Formatter("[%(asctime)s] %(name)s [%(levelname)s] %(filename)s LINE %(lineno)d:  %(message)s")
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        self.addHandler(stream_handler)

        file_handler = logging.FileHandler(BT_LOG_FILE)
        file_handler.setFormatter(formatter)
        self.addHandler(file_handler)

bt_log = BTLogger()

def bt_user_print(*args, **kwargs):
    sep = kwargs.get("sep", " ")
    end = kwargs.get("end", "")
    message = sep.join(map(str, args)) + end
    bt_log.info(message)


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


    bt_log.debug('debug')
    bt_log.info('info')
    bt_log.warning('warning')
    bt_log.error('error')
    bt_log.fatal('fatal')
    bt_log.critical('critical')

    a = BTLogger()
    b = BTLogger()
    print(id(a))
    print(id(b))
    a.debug('xxx')
    b.debug('yyy')

