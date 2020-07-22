# -*- coding：utf-8 -*-#

#--------------------------------------------------------------
#NAME:          shceduler.py
#Description:   定时获取wind数据
#Author:        xuezy
#Date:          2019/9/20
#--------------------------------------------------------------
#非阻塞方式
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import time as time1
import pymysql
import sys
import traceback
import pandas as pd

from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
import logging
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from WindPy import *
w.start()

# 用来操作数据库的类
class MySQLDB(object):
    # 类的初始化
    def __init__(self):
        # 本地连接
        self.host = 'localhost'
        self.port = 3306  # 端口号
        self.user = 'root'  # 用户名
        self.password = "root"  # 密码
        self.db = "wind"  # 库

        # 99连接
        # self.host = '192.168.12.99'
        # self.port = 3306  # 端口号
        # self.user = 'root'  # 用户名
        # self.password = "Uecom@server"  # 密码
        # self.db = "gtzb_prod"  # 库

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


# 配置日志记录信息，日志文件在当前路径，文件名为 “scheduler-log.txt”
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='scheduler-log.txt',
                    filemode='a'
                    )


# 定义一个事件监听，出现意外情况打印相关信息报警
def my_listener(event):
    if event.exception:
        print('executed false，time：', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        logging.info('executed false，time：%s' % datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    else:
        print('executed successfully，time：', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        logging.info('executed successfully，time：%s' % datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


# ----------------------------------------------------- 财务数据 start -----------------------------------------------------


def insertSpeDateCwsj(windCode, dt):
    '''
    导入指定日期的数据，一天拿2次，取到了不再获取
    :param windCode: wind编码，如：600061.SH
    :param dt: 日期参数
    :return:
    '''
    dateParam = dt.strftime('%Y%m%d')
    dateParamFormat = dt.strftime('%Y-%m-%d')
    mysql = MySQLDB()
    mysql.connectMysql()
    selectSql = "select * from wind_jrjyl where code = '" + windCode + "' and trade_date = '" + dateParamFormat + "'"
    result = mysql.select_one(selectSql)
    if result == None:
        # 获取当前年份
        currentYear = datetime.now().year
        # 拼接wss函数尾部内容
        param = "tradeDate=" + dateParam + ";priceAdj=U;cycle=D;year=" + str(currentYear)

        '''
        wss多维数据：只能取指定日期的数据
        windcode    编码
        sec_name    证券名称
        open        开盘价
        close       收盘价
        high        最高价
        low         最低价
        volume      成交量
        pct_chg     涨跌幅(%)
        turn        换手率(%)
        pe_ttm      市盈率PE(TTM)
        pb_lf       市净率PB(LF,内地)
        pb_mrq      市净率PB(MRQ)
        mkt_cap_ard 总市值(元) ev2
        amt         成交额(元)
        pe_est      预测PE
        est_eps                     预测EPS(平均)
        west_eps_fy1_1m             EPS一致预测-30天变化率
        west_eps_fy1_3m             EPS一致预测-90天变化率
        west_eps_fy1_6m             EPS一致预测-180天变化率
        est_netprofit               预测归母净利润(平均,单位：元)
        west_netprofit_fy1_1m       归母净利润一致预测-30天变化率
        west_netprofit_fy1_3m       归母净利润一致预测-90天变化率
        west_netprofit_fy1_6m       归母净利润一致预测-180天变化率
        '''
        # w.wss("600061.SH",
        #       "mkt_cap_ard,est_eps,west_eps_fy1_1m,west_eps_fy1_3m,west_eps_fy1_6m,est_netprofit,west_netprofit_fy1_1m,west_netprofit_fy1_3m,west_netprofit_fy1_6m",
        #       "unit=1;tradeDate=20191115;year=2019")
        data = w.wss(windCode, "windcode, sec_name, open, close, high, low, volume, pct_chg, turn, pe_ttm, pb_lf, pb_mrq, mkt_cap_ard, amt, pe_est,est_eps, west_eps_fy1_1m, west_eps_fy1_3m, west_eps_fy1_6m, est_netprofit, west_netprofit_fy1_1m, west_netprofit_fy1_3m, west_netprofit_fy1_6m", param)

        errorCode = data.ErrorCode
        if errorCode != 0:
            if errorCode == -40522017:
                print('从wind获取数据出错，错误代码：', errorCode, '，数据提取量超限')
                logging.error('从wind获取数据出错，错误代码：' + str(errorCode) + '，数据提取量超限')
            else:
                print('从wind获取数据出错，错误代码：', errorCode)
                logging.error('从wind获取数据出错，错误代码：' + str(errorCode))
        else:
            # 代码
            code = data.Data[0][0]
            # 简称
            shortName = data.Data[1][0]
            # 开盘价(元)
            list2 = data.Data[2]
            temp2 = "" if (list2[0]) == None else str(list2[0])
            # 收盘价(元)
            list3 = data.Data[3]
            temp3 = "" if (list3[0]) == None else str(list3[0])
            # 最高价(元)
            list4 = data.Data[4]
            temp4 = "" if (list4[0]) == None else str(list4[0])
            # 最低价(元)
            list5 = data.Data[5]
            temp5 = "" if (list5[0]) == None else str(list5[0])
            # 成交量(股)
            list6 = data.Data[6]
            temp6 = "" if (list6[0]) == None else str(list6[0])
            # 涨跌幅(%)
            list7 = data.Data[7]
            temp7 = "" if (list7[0]) == None else str(list7[0])
            # 换手率(%)
            list8 = data.Data[8]
            temp8 = "" if (list8[0]) == None else str(list8[0])
            # 市盈率PE(TTM)
            list9 = data.Data[9]
            temp9 = "" if (list9[0]) == None else str(list9[0])
            # 市净率PB(LF,内地)
            list10 = data.Data[10]
            temp10 = "" if (list10[0]) == None else str(list10[0])
            # 市净率PB(MRQ)
            list11 = data.Data[11]
            temp11 = "" if (list11[0]) == None else str(list11[0])
            # 总市值(元)
            list12 = data.Data[12]
            temp12 = "" if (list12[0]) == None else str(list12[0])
            # 成交额
            list13 = data.Data[13]
            temp13 = "" if (list13[0]) == None else str(list13[0])
            # 预测PE
            list14 = data.Data[14]
            temp14 = "" if (list14[0]) == None else str(list14[0])

            # 预测EPS(平均)
            list15 = data.Data[15]
            temp15 = "" if (list15[0]) == None else str(list15[0])
            # EPS一致预测 - 30天变化率
            list16 = data.Data[16]
            temp16 = "" if (list16[0]) == None else str(list16[0])
            # EPS一致预测-90天变化率
            list17 = data.Data[17]
            temp17 = "" if (list17[0]) == None else str(list17[0])
            # EPS一致预测-180天变化率
            list18 = data.Data[18]
            temp18 = "" if (list18[0]) == None else str(list18[0])
            # 预测归母净利润(平均,单位：元)
            list19 = data.Data[19]
            temp19 = "" if (list19[0]) == None else str(list19[0])
            # 归母净利润一致预测-30天变化率
            list20 = data.Data[20]
            temp20 = "" if (list20[0]) == None else str(list20[0])
            # 归母净利润一致预测-90天变化率
            list21 = data.Data[21]
            temp21 = "" if (list21[0]) == None else str(list21[0])
            # 归母净利润一致预测-180天变化率
            list22 = data.Data[22]
            temp22 = "" if (list22[0]) == None else str(list22[0])

            # 市净率PB(MRQ) 和 总市值(元) 之外的属性 均不为空 写入该条数据
            if temp2 != "" and temp3 != "" and temp4 != "" and temp5 != "" and temp6 != "" and temp7 != "" and temp8 != "" and temp9 != "" and temp10 != "" and temp13 != "":
                tempStr = '({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23})'.format(
                    "'" + code + "'",
                    "'" + shortName + "'",
                    "'" + dateParamFormat + "'",
                    "'" + temp2 + "'",
                    "'" + temp3 + "'",
                    "'" + temp4 + "'",
                    "'" + temp5 + "'",
                    "'" + temp6 + "'",
                    "'" + temp7 + "'",
                    "'" + temp8 + "'",
                    "'" + temp9 + "'",
                    "'" + temp10 + "'",
                    "'" + temp11 + "'",
                    "'" + temp12 + "'",
                    "'" + temp13 + "'",
                    "'" + temp14 + "'",
                    "'" + temp15 + "'",
                    "'" + temp16 + "'",
                    "'" + temp17 + "'",
                    "'" + temp18 + "'",
                    "'" + temp19 + "'",
                    "'" + temp20 + "'",
                    "'" + temp21 + "'",
                    "'" + temp22 + "'"
                )
                # 插入操作
                sql = "insert into wind_jrjyl(CODE,SHORT_NAME,TRADE_DATE,OPEN_PRICE,CLOSE_PRICE,MAX_PRICE,MIN_PRICE,VOL,CHG,TURNOVER_RATE,PE_TTM,PB_LF,PB_MRQ,EV,AMT,PE_EST,EST_EPS,EPS_1M,EPS_3M,EPS_6M,EST_NETPROFIT,NETPROFIT_1M,NETPROFIT_3M,NETPROFIT_6M) values" + tempStr
                n = mysql.insert(sql)
                print('写入条数：', n, '时间：', dateParamFormat)
                logging.info('写入条数：' + str(n) + ',时间：' + dateParamFormat)
            else:
                print('wind编码：', windCode, ' 数据不全，不写入。')
                logging.info('wind编码：' + windCode + ' 数据不全，不写入。')
    else:
        print('已存在当前交易日数据')
        logging.info('已存在当前交易日数据')
    # 关闭连接
    mysql.conn.close()

def saveTaskCwsj():
    '''
    批量写入指定日期数据
    :return:
    '''
    # Logger.make_print_to_file(path="./log/")
    print('************************ 写入安信证券财务数据 start ************************')
    logging.info('************************ 写入安信证券财务数据 start ************************')
    # 获取前3天时间，有不写入，无则写入
    dt = datetime.now() + timedelta(days=-1)
    dt2 = datetime.now() + timedelta(days=-2)
    dt3 = datetime.now() + timedelta(days=-3)
    # dt4 = datetime.now() + timedelta(days=-4)
    # dt5 = datetime.now() + timedelta(days=-5)
    dtList = [dt, dt2, dt3]

    windCode = ["600061.SH", "882501.WI", "600705.SH", "600390.SH", "002423.SZ", "000617.SZ", "600958.SH", "601901.SH", "601788.SH", "601377.SH", "000001.SH", "002736.SZ"]
    for item in windCode:
        for dt in dtList:
            print('写入wind编码为', item, '的数据，日期：', dt.strftime('%Y-%m-%d'), ':')
            logging.info('写入wind编码为' + item + '的数据，日期：' + dt.strftime('%Y-%m-%d') + ':')
            insertSpeDateCwsj(item, dt)
            # 写入时间，间隔2秒
            time1.sleep(2)
    print('************************ 写入安信证券财务数据 end ************************')
    logging.info('************************ 写入安信证券财务数据 end ************************')
# ----------------------------------------------------- 财务数据 end -----------------------------------------------------


# ----------------------------------------------------- 大宗交易 start -----------------------------------------------------


def insertBatchDzjy(startDate, endDate):
    '''
    导入国投资本 指定日期范围的数据，参数为字符串
    :param startDate: 开始日期，如 '1997-05-19'
    :param endDate: 结束日期，如 '2019-09-12'
    :return:
    '''
    windCode = '600061.SH'
    '''
    wset数据集：可以获取一个时间段内的数据
    windcode    编码
    name        证券名称
    trade_date  交易日期
    price       成交价(元)
    pre_close   前一交易日收盘价(元)
    close       当日收盘价(元)
    volume      成交量(万股)
    amount      成交额(万元)
    buy_exchange       买方营业部
    sell_exchange      卖方营业部
    '''
    data = w.wset("blocktraderecord","startdate=" + startDate + ";enddate=" + endDate + ";sectorid=a001010100000000;field=wind_code,name,trade_date,price,pre_close,close,volume,amount,buy_exchange,sell_exchange")

    errorCode = data.ErrorCode
    if errorCode != 0:
        if errorCode == -40522017:
            print('从wind获取数据出错，错误代码：', errorCode, '，数据提取量超限')
            logging.error('从wind获取数据出错，错误代码：' + str(errorCode) +  '，数据提取量超限')
        else:
            print('从wind获取数据出错，错误代码：', errorCode)
            logging.error('从wind获取数据出错，错误代码：' + str(errorCode))
    else:
        # 代码
        list = data.Data[0]
        # 简称
        list1 = data.Data[1]
        # 交易日期
        list2 = data.Data[2]
        # 成交价（元）
        list3 = data.Data[3]
        # 前一个交易日收盘价(元)
        list4 = data.Data[4]
        # 当日收盘价(元)
        list5 = data.Data[5]
        # 成交量(万股)
        list6 = data.Data[6]
        # 成交额（万元）
        list7 = data.Data[7]
        # 买方营业部
        list8 = data.Data[8]
        # 卖方营业部
        list9 = data.Data[9]

        # 获取数据量
        size = len(list)

        if size > 0:
            mysql = MySQLDB()
            mysql.connectMysql()
            i = 0
            appendStr = ''
            while i < size:
                if list[i] == windCode:
                    # 简称
                    temp1 = "" if (list1[i]) == None else str(list1[i])
                    # 交易日期
                    temp2 = "" if (list2[i]) == None else str(list2[i].strftime('%Y-%m-%d'))
                    # 成交价（元）
                    temp3 = "" if (list3[i]) == None else str(list3[i])
                    # 前一个交易日收盘价(元)
                    temp4 = "" if (list4[i]) == None else str(list4[i])
                    # 当日收盘价(元)
                    temp5 = "" if (list5[i]) == None else str(list5[i])
                    # 成交量(万股)
                    temp6 = "" if (list6[i]) == None else str(list6[i])
                    # 成交额（万元）
                    temp7 = "" if (list7[i]) == None else str(list7[i])
                    # 买方营业部
                    temp8 = "" if (list8[i]) == None else str(list8[i])
                    # 卖方营业部
                    temp9 = "" if (list9[i]) == None else str(list9[i])

                    # 根据代码和交易日期，判断是否已经获取了当前数据，获取了不再写入，没获取写入则写入当前数据
                    selectSql = "select * from wind_dzjy where code = '" + windCode + "' and trade_date = '" + temp2 + "'"
                    result = mysql.select_one(selectSql)
                    if result == None:
                        tempStr = '({0},{1},{2},{3},{4},{5},{6},{7},{8},{9}),'.format(
                                    "'" + windCode + "'",
                                    "'" + temp1 + "'",
                                    "'" + temp2 + "'",
                                    "'" + temp3 + "'",
                                    "'" + temp4 + "'",
                                    "'" + temp5 + "'",
                                    "'" + temp6 + "'",
                                    "'" + temp7 + "'",
                                    "'" + temp8 + "'",
                                    "'" + temp9 + "'"
                                )
                        appendStr = appendStr + tempStr
                i += 1

            # 有数据，做插入操作
            if(len(appendStr) != 0):

                # 写入语句
                sql = "insert into wind_dzjy(CODE,SHORT_NAME,TRADE_DATE,PRICE,PRE_CLOSE,CLOSE,VOL,AMOUNT,BUY_EXCHANGE,SELL_EXCHANGE) values"
                insertSql = sql + appendStr[0:len(appendStr) - 1]
                n = mysql.insert(insertSql)
                print("写入条数：", n , ',时间：' , datetime.now().strftime('%Y%m%d'))
                logging.info('写入条数：' + str(n) + ',时间：' + datetime.now().strftime('%Y%m%d'))
                # 关闭连接
                mysql.conn.close()


def saveTaskDzjy():
    '''
    定时任务用：获取近一年的数据，有则不在写入，没有的数据写入
    :return:
    '''
    # 获取近2天时间数据，有不写入，无则写入
    dt = datetime.now() + timedelta(days=-1)
    startTime = dt.strftime('%Y-%m-%d')
    endTime = datetime.now().strftime('%Y-%m-%d')
    print('************************ 写入[国投资本]大宗交易数据 start ************************')
    logging.info('************************ 写入[国投资本]大宗交易数据 start ************************')
    insertBatchDzjy(startTime, endTime)
    print('************************ 写入[国投资本]大宗交易数据 end ************************')
    logging.info('************************ 写入[国投资本]大宗交易数据 end ************************')
# ----------------------------------------------------- 大宗交易 end -----------------------------------------------------


# -------------------------------------------- 国投资本-上市公司业绩指标 start --------------------------------------------------
def insertYjzb(windCode, dateParam):
    '''
    导入上市公司（国投资本） 业绩指标指定日期范围的数据，参数为字符串
    :param windCode: wind公司编码   国投资本： '600061.SH'
    :param dateParam: 日期
    :return:
    '''
    # 数据时间，格式：年-月
    dataTime = dateParam[0:7]
    mysql = MySQLDB()
    mysql.connectMysql()
    # 根据交易日期，判断是否已经获取了当前数据，获取了不再写入，没获取写入则写入当前数据
    selectSql = "select * from jycw_ssgs_yjzb where data_time = '" + dataTime + "'"
    result = mysql.select_one(selectSql)
    # 不存在则写入数据
    if result == None:
        '''
        wss多维数据：获取指定时间的数据
        eps_basic               每股收益EPS-基本
        eps_diluted             每股收益EPS-稀释
        bps                     每股净资产BPS
        roe_basic               净资产收益率ROE(加权)
        roe_diluted             净资产收益率ROE(摊薄)
        debttoassets            资产负债率
        longdebttolongcaptial   长期资本负债率
        current                 流动比率
        quick                   速动比率
        ocftointerest           现金流量利息保障倍数
        eps_basic               每股收益-基本（EPS）
        '''
        data = w.wss(windCode, "eps_basic,eps_diluted,bps,roe_basic,roe_diluted,debttoassets,longdebttolongcaptial,current,quick,ocftointerest,eps_basic",
              "rptDate=" + dateParam + ";currencyType=")
        errorCode = data.ErrorCode
        if errorCode != 0:
            if errorCode == -40522017:
                print('从wind获取数据出错，错误代码：', errorCode, '，数据提取量超限')
                logging.error('从wind获取数据出错，错误代码：' + str(errorCode) +  '，数据提取量超限')
            else:
                print('从wind获取数据出错，错误代码：', errorCode)
                logging.error('从wind获取数据出错，错误代码：' + str(errorCode))
        else:
            # 每股收益EPS-基本
            list = data.Data[0]
            temp = "" if (list[0]) == None else str(list[0])
            # 每股收益EPS-稀释
            list1 = data.Data[1]
            temp1 = "" if (list1[0]) == None else str(list1[0])
            # 每股净资产BPS
            list2 = data.Data[2]
            temp2 = "" if (list2[0]) == None else str(list2[0])
            # 净资产收益率ROE(加权)
            list3 = data.Data[3]
            temp3 = "" if (list3[0]) == None else str(list3[0])
            # 净资产收益率ROE(摊薄)
            list4 = data.Data[4]
            temp4 = "" if (list4[0]) == None else str(list4[0])
            # 资产负债率
            list5 = data.Data[5]
            temp5 = "" if (list5[0]) == None else str(list5[0])
            # 长期资本负债率
            list6 = data.Data[6]
            temp6 = "" if (list6[0]) == None else str(list6[0])
            # 流动比率
            list7 = data.Data[7]
            temp7 = "" if (list7[0]) == None else str(list7[0])
            # 速动比率
            list8 = data.Data[8]
            temp8 = "" if (list8[0]) == None else str(list8[0])
            # 现金流量利息保障倍数  为空则写入0
            list9 = data.Data[9]
            temp9 = "0" if (list9[0]) == None or (str(list9[0])) == 'nan' else str(list9[0])
            # 每股收益-基本（EPS）
            list10 = data.Data[10]
            temp10 = "" if (list10[0]) == None or (str(list10[0])) == 'nan' else str(list10[0])

            # 属性都为空，则没有数据，不写入
            if temp == "" and temp1 == "" and temp2 == "" and temp3 == "" and temp4 == "" and temp5 == "" and temp6 == "" and temp7 == "" and temp8 == "" and temp9 == "0":
                print('wind编码：', windCode, ' 无数据，数据时间：', dateParam, ';获取数据的时间：', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                logging.info('wind编码：' + windCode + ' 无数据，数据时间：' + dateParam +  ';获取数据的时间：' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            else:
                tempStr = '({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11})'.format(
                            "'" + temp + "'",
                            "'" + temp1 + "'",
                            "'" + temp2 + "'",
                            "'" + temp3 + "'",
                            "'" + temp4 + "'",
                            "'" + temp5 + "'",
                            "'" + temp6 + "'",
                            "'" + temp7 + "'",
                            "'" + temp8 + "'",
                            "'" + temp9 + "'",
                            "'" + temp10 + "'",
                            "'" + dataTime + "'"
                        )
                # 写入语句
                sql = "insert into jycw_ssgs_yjzb(MGSYEPS_JB,MGSYEPS_XS,MGJZCBPS,JZCSYLROE_JQ,JZCSYLROE_TB,ZCFZL,CQZBFZL,LDBL,SDBL,XJLLLLBZBS,EPS_BASIC,DATA_TIME) values"
                insertSql = sql + tempStr
                n = mysql.insert(insertSql)
                print('写入条数：', n, ';数据时间：', dateParam)
                logging.info('写入条数：' + str(n) + ';数据时间：' + dateParam)
    else:
        print('已存在当前交易日数据')
        logging.info('已存在当前交易日数据')
    # 关闭连接
    mysql.conn.close()


def saveTaskYjzb():
    '''
    定时任务用：获取近两个季度的数据，有则不在写入，没有的数据写入
    :return:
    '''
    print('************************ 写入上市公司(国投资本)业绩指标数据 start ************************')
    logging.info('************************ 写入上市公司(国投资本)业绩指标数据 start ************************')
    # 国投资本
    windCode = '600061.SH'
    dateList = []
    year = datetime.now().year
    month = datetime.now().month
    day = datetime.now().day
    # 获取时间
    if month <= 3:
        preDateTime = str(year - 1) + '-09-30'
        preDateTime2 = str(year - 1) + '-12-31'
        dateList.append(preDateTime)
        dateList.append(preDateTime2)
    elif month <= 6:
        preDateTime = str(year - 1) + '-12-31'
        preDateTime2 = str(year) + '-03-31'
        dateList.append(preDateTime)
        dateList.append(preDateTime2)
    elif month <= 9:
        preDateTime = str(year) + '-03-31'
        preDateTime2 = str(year) + '-06-30'
        dateList.append(preDateTime)
        dateList.append(preDateTime2)
    elif month <= 12:
        preDateTime = str(year) + '-06-30'
        preDateTime2 = str(year) + '-09-30'
        dateList.append(preDateTime)
        dateList.append(preDateTime2)

    for item in dateList:
        print('写入wind编码为', windCode, '的数据，日期：', item, ':')
        logging.info('写入wind编码为' + windCode + '的数据，日期：' + item + ':')
        insertYjzb(windCode, item)
        # 写入时间，间隔2秒
        time1.sleep(2)

    print('************************ 写入上市公司(国投资本)业绩指标数据 end ************************')
    logging.info('************************ 写入上市公司(国投资本)业绩指标数据 end ************************')
# --------------------------------------------国投资本-上市公司业绩指标 end -----------------------------------------------------


# --------------------------------------------国投资本-融资融券 start -----------------------------------------------------
def insertSpeDateRzrq(windCode, dt):
    '''
    导入指定日期的数据，一天拿2次，取到了不再获取
    :param windCode: wind编码，如：600061.SH
    :param dt: 日期参数
    :return:
    '''
    dateParam = dt.strftime('%Y%m%d')
    dateParamFormat = dt.strftime('%Y-%m-%d')
    mysql = MySQLDB()
    mysql.connectMysql()
    selectSql = "select * from wind_rzrq where trade_date = '" + dateParamFormat + "'"
    result = mysql.select_one(selectSql)
    if result == None:
        # 获取当前年份
        currentYear = datetime.now().year
        # 拼接wss函数尾部内容
        param = "unit=1;tradeDate=" + dateParam
        '''
        wss多维数据：只能取指定日期的数据
        mrg_long_amt            融资买入(周期=日)-买入额(元)
        mrg_long_repay          融资买入(周期=日)-偿还额(元)
        mrg_short_vol           融券卖出(周期=日)-卖出量(股)
        mrg_short_vol_repay     融券卖出(周期=日)-偿还量(股)
        mrg_long_bal            期末余额(余量)-融资余额(元)
        mrg_short_bal           期末余额(余量)-融券余额(元)
        mrg_short_vol_bal       期末余额(余量)-融券余量(股)
        mkt_cap_ashare          A股市值(不含限售股)(元)
        '''
        data = w.wss(windCode,"mrg_long_amt,mrg_long_repay,mrg_short_vol,mrg_short_vol_repay,mrg_long_bal,mrg_short_bal,mrg_short_vol_bal,mkt_cap_ashare",param)
        errorCode = data.ErrorCode
        if errorCode != 0:
            if errorCode == -40522017:
                print('从wind获取数据出错，错误代码：', errorCode, '，数据提取量超限')
                logging.error('从wind获取数据出错，错误代码：' + str(errorCode) + '，数据提取量超限')
            else:
                print('从wind获取数据出错，错误代码：', errorCode)
                logging.error('从wind获取数据出错，错误代码：' + str(errorCode))
        else:
            # 融资买入(周期=日)-买入额(元)
            list = data.Data[0]
            temp = "" if pd.isnull(list[0]) else str(list[0])
            # 融资买入(周期=日)-偿还额(元)
            list1 = data.Data[1]
            temp1 = "" if pd.isnull(list1[0]) else str(list1[0])
            # 融券卖出(周期=日)-卖出量(股)
            list2 = data.Data[2]
            temp2 = "" if pd.isnull(list2[0]) else str(list2[0])
            # 融券卖出(周期=日)-偿还量(股)
            list3 = data.Data[3]
            temp3 = "" if pd.isnull(list3[0]) else str(list3[0])
            # 期末余额(余量)-融资余额(元)
            list4 = data.Data[4]
            temp4 = "" if pd.isnull(list4[0]) else str(list4[0])
            # 期末余额(余量)-融券余额(元)
            list5 = data.Data[5]
            temp5 = "" if pd.isnull(list5[0]) else str(list5[0])
            # 期末余额(余量)-融券余量(股)
            list6 = data.Data[6]
            temp6 = "" if pd.isnull(list6[0]) else str(list6[0])
            # A股市值(不含限售股)(元)
            list7 = data.Data[7]
            temp7 = "" if pd.isnull(list7[0]) else str(list7[0])

            if temp != "" and temp1 != "" and temp2 != "" and temp3 != "" and temp4 != "" and temp5 != "" and temp6 != "" and temp7 != "":
                tempStr = '({0},{1},{2},{3},{4},{5},{6},{7},{8})'.format(
                    "'" + dateParamFormat + "'",
                    "'" + temp + "'",
                    "'" + temp1 + "'",
                    "'" + temp2 + "'",
                    "'" + temp3 + "'",
                    "'" + temp4 + "'",
                    "'" + temp5 + "'",
                    "'" + temp6 + "'",
                    "'" + temp7 + "'"
                )
                # 插入操作
                sql = "insert into wind_rzrq(TRADE_DATE,RZMR_MRE,RZMR_CHE,RQMC_MCL,RQMC_CHL,QMYE_RZYE,QMYE_RQYE,QMYE_RQYL,AGSZ) values" + tempStr
                n = mysql.insert(sql)
                print('写入条数：', n, '时间：', dateParamFormat)
                logging.info('写入条数：' + str(n) + ',时间：' + dateParamFormat)
            else:
                print('wind编码：', windCode, ' 数据不全，不写入。')
                logging.info('wind编码：' + windCode + ' 数据不全，不写入。')
    else:
        print('已存在当前交易日数据')
        logging.info('已存在当前交易日数据')
    # 关闭连接
    mysql.conn.close()

def saveTaskRzrq():
    '''
    批量写入指定日期数据
    :return:
    '''
    # Logger.make_print_to_file(path="./log/")
    print('************************ 写入融资融券数据 start ************************')
    logging.info('************************ 写入融资融券数据 start ************************')
    # 获取前3天时间，有不写入，无则写入
    dt = datetime.now() + timedelta(days=-1)
    dt2 = datetime.now() + timedelta(days=-2)
    dt3 = datetime.now() + timedelta(days=-3)
    dtList = [dt, dt2, dt3]

    windCode = "600061.SH"
    for dt in dtList:
        print('写入wind编码为', windCode, '的数据，日期：', dt.strftime('%Y-%m-%d'), ':')
        logging.info('写入wind编码为' + windCode + '的数据，日期：' + dt.strftime('%Y-%m-%d') + ':')
        insertSpeDateRzrq(windCode, dt)
        # 写入时间，间隔2秒
        time1.sleep(2)
    print('************************ 写入融资融券数据 end ************************')
    logging.info('************************ 写入融资融券数据 end ************************')
# --------------------------------------------国投资本-融资融券 end -----------------------------------------------------

# --------------------------------------------违约债地域分布数据 start -----------------------------------------------------
def insertSpeDateWyzdyfb(dt):
    '''
    导入指定日期的数据
    :param dt: 日期参数
    :return:
    '''
    dateParamFormat = dt.strftime('%Y-%m-%d')
    mysql = MySQLDB()
    mysql.connectMysql()
    selectSql = "select * from wind_wyzdyfb where trade_date = '" + dateParamFormat + "'"
    result = mysql.select_list(selectSql)
    if result.__len__() == 0:
        # 拼接wset函数尾部内容
        param = "enddate=" + dateParamFormat

        '''
        wset数据集：只能取指定日期的数据
        '''
        # w.wset("defaultbondbyprovince","enddate=2020-05-12")
        data = w.wset("defaultbondbyprovince",param)
        errorCode = data.ErrorCode
        if errorCode != 0:
            if errorCode == -40522017:
                print('从wind获取数据出错，错误代码：', errorCode, '，数据提取量超限')
                logging.error('从wind获取数据出错，错误代码：' + str(errorCode) + '，数据提取量超限')
            else:
                print('从wind获取数据出错，错误代码：', errorCode)
                logging.error('从wind获取数据出错，错误代码：' + str(errorCode))
        else:
            # 省（直辖市）id
            list = data.Data[0]
            # 省（直辖市）
            list1 = data.Data[1]
            # 违约债只数
            list2 = data.Data[2]
            # 违约债券余额（亿元）
            list3 = data.Data[3]
            # 余额违约率（%）
            list4 = data.Data[4]
            # 违约发行人个数
            list5 = data.Data[5]
            # 发行人个数违约比率（%）
            list6 = data.Data[6]

            # 获取数据量
            size = len(list)

            if size > 0:
                i = 0
                appendStr = ''
                while i < size:
                    # 省（直辖市）id
                    temp = "" if (list[i]) == None else str(list[i])
                    # 省（直辖市）
                    temp1 = "" if (list1[i]) == None else str(list1[i])
                    # 违约债只数
                    temp2 = "" if (list2[i]) == None else str(list2[i])
                    # 违约债券余额（亿元）
                    temp3 = "" if (list3[i]) == None else str(list3[i])
                    # 余额违约率（%）
                    temp4 = "" if (list4[i]) == None else str(list4[i])
                    # 违约发行人个数
                    temp5 = "" if (list5[i]) == None else str(list5[i])
                    # 发行人个数违约比率（%）
                    temp6 = "" if (list6[i]) == None else str(list6[i])

                    # 属性 均不为空 写入该条数据
                    if temp != "" and temp1 != "" and temp2 != "" and temp3 != "" and temp4 != "" and temp5 != "" and temp6 != "" :
                        tempStr = '({0},{1},{2},{3},{4},{5},{6},{7}),'.format(
                            "'" + dateParamFormat + "'",
                            "'" + temp + "'",
                            "'" + temp1 + "'",
                            "'" + temp2 + "'",
                            "'" + temp3 + "'",
                            "'" + temp4 + "'",
                            "'" + temp5 + "'",
                            "'" + temp6 + "'"
                        )
                        appendStr = appendStr + tempStr
                    i += 1

                # 有数据，做插入操作
                if (len(appendStr) != 0):
                    mysql = MySQLDB()
                    mysql.connectMysql()
                    # 写入语句
                    sql = "insert into wind_wyzdyfb(TRADE_DATE,PROVINCE_SECTOR_ID,PROVINCE,BONDNUM,OUTSTANDING,OUTSTANDING_RATIO,PUBLISHER_NUM,PUBLISHER_NUM_RATIO) values"
                    insertSql = sql + appendStr[0:len(appendStr) - 1]
                    n = mysql.insert(insertSql)
                    print('写入条数：', n, ';数据时间：', dateParamFormat)
                    logging.info('写入条数：' + str(n) + ';数据时间：' + dateParamFormat)
    else:
        print('已存在当前交易日数据')
        logging.info('已存在当前交易日数据')
    # 关闭连接
    mysql.conn.close()

def saveTaskWyzdyfb():
    '''
    批量写入指定日期数据
    :return:
    '''
    print('************************ 写入违约债地域分布数据 start ************************')
    logging.info('************************ 写入违约债地域分布数据 start ************************')

    # 获取前3天时间，有不写入，无则写入
    dt = datetime.now()+ timedelta(days=-1)
    dt1 = datetime.now() + timedelta(days=-2)
    dt2 = datetime.now() + timedelta(days=-3)
    dtList = [dt, dt1, dt2]
    for dt in dtList:
        print('写入数据，日期：', dt.strftime('%Y-%m-%d'))
        logging.info('写入数据，日期：' + dt.strftime('%Y-%m-%d'))
        insertSpeDateWyzdyfb(dt)
        # 写入时间，间隔2秒
        time1.sleep(2)
    print('************************ 写入违约债地域分布数据 end ************************')
    logging.info('************************ 写入违约债地域分布数据 end ************************')
# --------------------------------------------违约债地域分布数据 end -----------------------------------------------------

if __name__ == '__main__':
    scheduler = BackgroundScheduler()

    # 添加监听
    scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    # 安信证券-财务报表（获取前3天的数据）
    #每天8:15、20：15各执行一次（获取近3天的数据，有数据了不再写入）;有了这个jitter参数，它会使每个任务提前或延后+30/-30秒范围去运行
    cwsj_trigger = CronTrigger(hour='15', minute='08', jitter=30)

    # submit_job(提交任务)时加1，在_run_job_success(任务运行成功)时减1。 当self._instances[job.id]大于job.max_instances抛出异常。
    # max_instances默认值为1，它表示id相同的任务实例数
    # misfire_grace_time允许容错的时间，单位为：s   解决was  miss  by 错误
    scheduler.add_job(saveTaskCwsj, cwsj_trigger, max_instances=10, misfire_grace_time=3600, id = "1")

    # 国投资本-大宗交易（获取近2天的数据）
    # dzjy_trigger = CronTrigger(hour='20', minute='30', jitter=30)
    # scheduler.add_job(saveTaskDzjy, dzjy_trigger)

    # 国投资本-上市公司业绩指标（获取近两个季度的数据）
    yjzb_trigger = CronTrigger(hour='15', minute='15', jitter=30)
    scheduler.add_job(saveTaskYjzb, yjzb_trigger, max_instances=10, misfire_grace_time=3600, id = "2")

    # 国投资本-融资融券（获取前3天的数据，有数据了不再写入）
    rzrq_trigger = CronTrigger(hour='15', minute='30', jitter=30)
    scheduler.add_job(saveTaskRzrq, rzrq_trigger, max_instances=10, misfire_grace_time=3600, id="3")

    # 违约债地域分布数据（获取前3天的数据，有数据了不再写入）
    wyzdyfb_trigger = CronTrigger(hour='15', minute='45', jitter=30)
    scheduler.add_job(saveTaskWyzdyfb, wyzdyfb_trigger, max_instances=10, misfire_grace_time=3600, id="4")

    # 启用 scheduler 模块的日记记录
    scheduler._logger = logging

    # 该部分调度是一个独立的线程
    scheduler.start()

    try:
        # 模拟主进程持续运行，每一天打印一次 86400
        while True:
            time1.sleep(1 * 60)
            print ('currenttime：',datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    except(KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print('Exit The Job!')