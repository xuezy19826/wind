# -*- coding：utf-8 -*-#

#--------------------------------------------------------------
#NAME:          dzjy
#Description:   获取国投资本-大宗交易数据
#Author:        xuezy
#Date:          2019/11/15
#--------------------------------------------------------------
from mysqlutil import MySQLDB
import datetime
import time as time1
import logging
from WindPy import *
w.start()

class Dzjy:

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
                    mysql = MySQLDB()
                    mysql.connectMysql()
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
        Dzjy.insertBatchDzjy(startTime, endTime)
        print('************************ 写入[国投资本]大宗交易数据 end ************************')
        logging.info('************************ 写入[国投资本]大宗交易数据 end ************************')

if __name__ == '__main__':
    # Dzjy.insertBatchDzjy('2009-12-16','2009-12-31')
    # Dzjy.insertBatchDzjy('2010-01-01', '2010-12-31')
    # Dzjy.insertBatchDzjy('2011-01-01', '2011-12-31')
    # Dzjy.insertBatchDzjy('2012-01-01', '2012-12-31')
    # Dzjy.insertBatchDzjy('2013-01-01', '2013-12-31')
    # Dzjy.insertBatchDzjy('2014-01-01', '2014-12-31')
    # Dzjy.insertBatchDzjy('2015-01-01', '2015-12-31')
    # Dzjy.insertBatchDzjy('2016-01-01', '2016-12-31')
    Dzjy.insertBatchDzjy('2019-11-15', '2019-11-26')