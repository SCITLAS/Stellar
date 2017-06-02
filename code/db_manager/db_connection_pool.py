# -*- coding: utf-8 -*-
#
# Copyright 2017 Xilosopher
#
# Author: Moro JoJo


import pymysql

mysql_config = {
    'user': 'liyuhuang',
    'password': '123456',
    'host': 'localhost',
    'database': 'db_dev',
    'charset': 'utf8'
}


class DBConnectionPool:
    #构建连接池实例
    def __init__(self, maxconnections, dbtype, config):
        from queue import Queue
        self._pool = Queue(maxconnections)
        self.config = config
        self.dbtype = dbtype
        self.maxconnections = maxconnections
        try:
            for i in range(maxconnections):
                self.fill_connection(self.create_connection())
        except Exception as e:
            raise e

    def fill_connection(self, conn):
        try:
            self._pool.put(conn)

        except Exception as e:
            raise "fillConnection error:"+str(e)

    def return_connection(self, conn):
        try:
            self._pool.put(conn)
        except Exception as e:
            raise "returnConnection error:"+str(e)

    def get_connection(self):
        try:
            return self._pool.get()
        except Exception as e:
            raise "getConnection error:"+str(e)

    def close_connection(self):
        try:
            self._pool.get().close()
            self.fill_connection(self.create_connection())
        except Exception as e:
            raise "CloseConnection error:"+str(e)

    def create_connection(self):
        if self.dbtype == 'mysql':
            try:
                conn = pymysql.connect(
                    user=self.config['user'],
                    passwd=self.config['password'],
                    host=self.config['host'],
                    db=self.config['database'],
                    charset=self.config['charset']
                )
                conn.ping()
            except Exception as e:
                print('Create MySQL connection exception: %s' % e)
                return None
            else:
                return conn
