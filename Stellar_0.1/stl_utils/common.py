# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


import os
from stl_utils.const import PROJECT_CODE_NAME


'''
常用辅助方法
'''


# bytes转str
def to_str(bytes_or_str):
    if isinstance(bytes_or_str, bytes):
        value = bytes_or_str.decode('utf-8')
    else:
        value = bytes_or_str
    return value  # instance of str


# str转bytes
def to_bytes(bytes_or_str):
    if isinstance(bytes_or_str, str):
        value = bytes_or_str.encode('utf-8')
    else:
        value = bytes_or_str
    return value  # instance of bytes


# 文件读取迭代类, 返回一个生成器
class StlReadVisits(object):
    def __init__(self, data_path):
        '''

        Parameters
        ----------
            data_path: 需要读取的数据文件路径

        Returns
        -------
            无
        '''
        self.data_path = data_path

    def __iter__(self):
        '''

        Returns
        -------
            line: 逐行迭代生成器
        '''
        with open(self.data_path) as f:
            for line in f:
                yield line


def data_handle_defensive(datas):
    '''
    处理datas容器中的数据, 要求输入参数datas是一个把__iter__实现为生成器的容器类.
    Parameters
    ----------
        datas: 数据容器
    Returns
    -------
        无
    Exception
        TypeError('Must supply a container')
    '''
    if iter(datas) is iter(datas):
        raise TypeError('Must supply a container')

    # TODO: 需要对数据做的处理都写在下面
    for line in datas:
        print(line)


def get_project_root():
    '''

    处理datas容器中的数据, 要求输入参数datas是一个把__iter__实现为生成器的容器类.

    Parameters
    ----------
        无
    Returns
    -------
        无
    Exception
        无
    '''
    thePath = os.getcwd()
    print(thePath)
    path_list = thePath.split(PROJECT_CODE_NAME)
    print(path_list[0]+PROJECT_CODE_NAME)


if __name__ == '__main__':
    print(to_bytes('xxx'))
    print(to_str('xxxx'))
    get_project_root()
