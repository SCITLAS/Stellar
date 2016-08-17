# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


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


class StlReadVisits(object):
    def __init__(self, data_path):
        self.data_path = data_path

    def __iter__(self):
        with open(self.data_path) as f:
            for line in f:
                yield line


if __name__ == '__main__':
    print(to_bytes('xxx'))
    print(to_str('xxxx'))
