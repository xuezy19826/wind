# -*- coding：utf-8 -*-#

#--------------------------------------------------------------
#NAME:          dzjy
#Description:   获取上司公司（国投资本）业绩指标
#Author:        xuezy
#Date:          2019/11/17
#--------------------------------------------------------------
from mysqlutil import MySQLDB
import datetime
import time as time1
import logging
from WindPy import *
w.start()

class Yjzb:

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

    def saveBatchYjzb():
        windCode = '600061.SH'
        dateList = [
            '2015-03-31', '2015-06-30', '2015-09-30', '2015-12-31',
            '2016-03-31', '2016-06-30', '2016-09-30', '2016-12-31',
            '2017-03-31', '2017-06-30', '2017-09-30', '2017-12-31',
            '2018-03-31', '2018-06-30', '2018-09-30', '2018-12-31',
            '2019-03-31', '2019-06-30', '2019-06-30'
        ]
        for item in dateList:
            print('写入wind编码为', windCode, '的数据，日期：', item, ':')
            logging.info('写入wind编码为' + windCode + '的数据，日期：' + item + ':')
            Yjzb.insertYjzb(windCode, item)
            # 写入时间，间隔2秒
            time1.sleep(2)

    def saveTaskYjzb():
        '''
        定时任务用：获取近两个季度的数据，有则不在写入，没有的数据写入
        :return:
        '''
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
        print('************************ 写入上市公司(国投资本)业绩指标数据 start ************************')
        logging.info('************************ 写入上市公司(国投资本)业绩指标数据 start ************************')
        for item in dateList:
            print('写入wind编码为', windCode, '的数据，日期：', item, ':')
            logging.info('写入wind编码为' + windCode + '的数据，日期：' + item + ':')
            Yjzb.insertYjzb(windCode, item)
            # 写入时间，间隔2秒
            time1.sleep(2)
        print('************************ 写入上市公司(国投资本)业绩指标数据 end ************************')
        logging.info('************************ 写入上市公司(国投资本)业绩指标数据 end ************************')

# if __name__ == '__main__':
   # Yjzb.saveBatchYjzb()
   #  Yjzb.saveTaskYjzb()