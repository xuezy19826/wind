# -*- coding：utf-8 -*-#

#--------------------------------------------------------------
#NAME:          main
#Description:  获取违约债地域分布
#Author:        xuezy
#Date:          2020/5/14
#--------------------------------------------------------------
from mysqlutil import MySQLDB
import datetime
import time as time1
import logging
from WindPy import *
w.start()


class Wyzdyfb:

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

    def saveTaskWyzdyfb(pa):
        '''
        批量写入指定日期数据
        :return:
        '''
        # Logger.make_print_to_file(path="./log/")
        print('************************ 写入违约债地域分布数据 start ************************')
        logging.info('************************ 写入违约债地域分布数据 start ************************')

        # 获取前5天时间，有不写入，无则写入
        dt = datetime.now()
        dt2 = datetime.now() + timedelta(days=-2)
        dt3 = datetime.now() + timedelta(days=-3)
        dt4 = datetime.now() + timedelta(days=-4)
        dt5 = datetime.now() + timedelta(days=-5)
        dtList = [dt, dt2, dt3, dt4, dt5]
        for dt in dtList:
            print('写入数据，日期：', dt.strftime('%Y-%m-%d'))
            logging.info('写入数据，日期：' + dt.strftime('%Y-%m-%d'))
            Wyzdyfb.insertSpeDateWyzdyfb(dt)
            # 写入时间，间隔2秒
            time1.sleep(2)
        print('************************ 写入违约债地域分布数据 end ************************')
        logging.info('************************ 写入违约债地域分布数据 end ************************')

    def saveBatchInfo(self):
        '''
        批量写入数据
        '''
        i = 1;
        # 139表示距离当前139天
        while i < 139:
            dt = datetime.now() + timedelta(days=-i)
            print('日期：', dt)
            Wyzdyfb.insertSpeDateCwsj(dt)
            i += 1;

if __name__ == '__main__':
    # 写入数据
    Wyzdyfb.saveTaskWyzdyfb(1)

    # 批量写入数据
    # Wyzdyfb.saveBatchInfo(1)


