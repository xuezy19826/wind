# -*- coding：utf-8 -*-#

# --------------------------------------------------------------
# NAME:          mysqlutil
# Description:   mysql工具类
# Author:        xuezy
# Date:          2019/8/29
# --------------------------------------------------------------
import pymysql
import sys
import traceback
import logging


# 用来操作数据库的类
class MySQLDB(object):
    # 类的初始化
    def __init__(self):
        # 本地连接
        # self.host = 'localhost'
        # self.port = 3306  # 端口号
        # self.user = 'root'  # 用户名
        # self.password = "root"  # 密码
        # self.db = "wind"  # 库

        # 99连接
        self.host = '192.168.12.99'
        self.port = 3306  # 端口号
        self.user = 'root'  # 用户名
        self.password = "Uecom@server"  # 密码
        self.db = "gtzb_test"  # 库

    # 连接数据库
    def connectMysql(self):
        try:
            self.conn = pymysql.connect(host=self.host, port=self.port, user=self.user,
                                        passwd=self.password, db=self.db, charset='utf8')
            self.cursor = self.conn.cursor()
        except:
            logging.info("connect mysql error.")
            raise ('connect mysql error.')

    def insert(self, sql):
        '''
        插入数据
        :param sql: sql语句
        :param value: 可变参数
        :return: 插入条数
        '''
        try:
            n = self.cursor.execute(sql)
            self.conn.commit()
        except:
            n = -1
            info = sys.exc_info()
            print('[MySQLDB==>insert] 异常：\n',info[0],':',info[1])
            print('参数：[sql]', sql);
            logging.info('[MySQLDB==>insert] 异常：\n' + info[0] + ':' + info[1] + '\n' +  '参数：[sql]' + sql)
        # self.conn.rollback()
        return n

    def delete(self, sql):
        '''
        删除数据
        :param sql: sql语句
        :param value:  可变参数
        :return:  删除的条数
        '''
        try:
            n = 1
            self.cursor.execute(sql)
            self.conn.commit()
        except:
            n = -1
            info = sys.exc_info()
            print('[MySQLDB==>delete] 异常：\n', info[0], ':', info[1])
            print('参数：[sql]' + sql);
            self.conn.rollback()
        # 关闭资源
        # self.close()
        return n

    def update(self, sql):
        '''
        修改数据
        :param sql:  sql语句
        :param value:  可变参数
        :return:  修改的条数
        '''
        try:
            n = 1
            self.cursor.execute(sql)
            self.conn.commit()
        except:
            n = -1
            info = sys.exc_info()
            print('[MySQLDB==>update] 异常：\n', info[0], ':', info[1])
            print('参数：[sql]' + sql);
            self.conn.rollback()
        # self.close()
        return n

    def select_one(self, sql):
        '''
        查询一条数据查询  fetchone
        :param sql:  sql语句
        :param value:  可变参数
        :return:
        '''
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchone()
        except:
            info = sys.exc_info()
            print('[MySQLDB==>select_one] 异常：\n', info[0], ':', info[1])
            print('参数：[sql]' + sql);
        # self.close()
        return result

    def select_list(self, sql):
        '''
        查询多条数据查询  fetchall
        :param sql:  sql语句
        :param value:  可变参数
        :return:
        '''
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            print(result)
        except:
            info = sys.exc_info()
            print('[MySQLDB==>select_list] 异常：\n', info[0], ':', info[1])
            print('参数：[sql]' + sql);
        # self.close()
        return result

    # 关闭连接
    def close(self):
        self.cursor.close()
        self.conn.close()

# 创建数据库操作类的实例
# mysql = MySQLDB()
# mysql.connectMysql()
# sql = "select * from test"
# mysql.select_one(sql
