# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


'''
Stellar own test back framework
'''


def test_back_test():
    from test_back.stellar import manager
    manager.run_simple_macd_strategy()


if __name__ == '__main__':
    test_back_test()
