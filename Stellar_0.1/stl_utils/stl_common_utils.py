# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


'''
常用辅助方法
'''


def to_str(bytes_or_str):
    if isinstance(bytes_or_str, bytes):
        value = bytes_or_str.decode('utf-8')
    else:
        value = bytes_or_str
    return value  # instance of str


def to_bytes(bytes_or_str):
    if isinstance(bytes_or_str, str):
        value = bytes_or_str.encode('utf-8')
    else:
        value = bytes_or_str
    return value  # instance of bytes


if __name__ == '__main__':
    print(to_bytes('xxx'))
    print(to_str('xxxx'))
