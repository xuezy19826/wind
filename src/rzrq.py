# -*- coding：utf-8 -*-#

#--------------------------------------------------------------
#NAME:          main
#Description:  获取 融资融券 数据
#Author:        xuezy
#Date:          2019/9/12
#--------------------------------------------------------------
from mysqlutil import MySQLDB
import datetime
import time as time1
import pandas as pd
import logging
from WindPy import *
w.start()


class Rzrq:
    def insertBatchRzrq(windCode, startDate, endDate):
        '''
        导入600061.SH (国投资本)、指定日期范围的数据，参数为字符串
        :param windCode: wind编码
        :param startDate: 开始日期，如 '1997-05-19'
        :param endDate: 结束日期，如 '2019-09-12'
        :return:
        '''

        '''
        wsd日期序列：可以获取一个时间段内的数据
        mrg_long_amt            融资买入(周期=日)-买入额(元)
        mrg_long_repay          融资买入(周期=日)-偿还额(元)
        mrg_short_vol           融券卖出(周期=日)-卖出量(股)
        mrg_short_vol_repay     融券卖出(周期=日)-偿还量(股)
        mrg_long_bal            期末余额(余量)-融资余额(元)
        mrg_short_bal           期末余额(余量)-融券余额(元)
        mrg_short_vol_bal       期末余额(余量)-融券余量(股)
        mkt_cap_ashare          A股市值(不含限售股)(元)
        '''
        data = w.wsd("600061.SH", "mrg_long_amt,mrg_long_repay,mrg_short_vol,mrg_short_vol_repay,mrg_long_bal,mrg_short_bal,mrg_short_vol_bal,mkt_cap_ashare", startDate, endDate, "unit=1;Currency=CNY")

        errorCode = data.ErrorCode
        if errorCode != 0:
            if errorCode == -40522017:
                print('从wind获取数据出错，错误代码：', errorCode, '，数据提取量超限')
                logging.error('从wind获取数据出错，错误代码：' + str(errorCode) + '，数据提取量超限')
            else:
                print('从wind获取数据出错，错误代码：', errorCode)
                logging.error('从wind获取数据出错，错误代码：' + str(errorCode))
        else:
            # 时间
            timeList = data.Times
            # 融资买入(周期=日)-买入额(元)
            list = data.Data[0]
            # 融资买入(周期=日)-偿还额(元)
            list1 = data.Data[1]
            # 融券卖出(周期=日)-卖出量(股)
            list2 = data.Data[2]
            # 融券卖出(周期=日)-偿还量(股)
            list3 = data.Data[3]
            # 期末余额(余量)-融资余额(元)
            list4 = data.Data[4]
            # 期末余额(余量)-融券余额(元)
            list5 = data.Data[5]
            # 期末余额(余量)-融券余量(股)
            list6 = data.Data[6]
            # A股市值(不含限售股)(元)
            list7 = data.Data[7]

            # 获取数据量
            size = len(timeList)

            if size > 0:
                i = 0
                appendStr = ''
                while i < size:
                    # 融资买入(周期=日)-买入额(元)
                    temp = "" if (list[i]) == None else str(list[i])
                    # 融资买入(周期=日)-偿还额(元)
                    temp1 = "" if (list1[i]) == None else str(list1[i])
                    # 融券卖出(周期=日)-卖出量(股)
                    temp2 = "" if (list2[i]) == None else str(list2[i])
                    # 融券卖出(周期=日)-偿还量(股)
                    temp3 = "" if (list3[i]) == None else str(list3[i])
                    # 期末余额(余量)-融资余额(元)
                    temp4 = "" if (list4[i]) == None else str(list4[i])
                    # 期末余额(余量)-融券余额(元)
                    temp5 = "" if (list5[i]) == None else str(list5[i])
                    # 期末余额(余量)-融券余量(股)
                    temp6 = "" if (list6[i]) == None else str(list6[i])
                    # A股市值(不含限售股)(元)
                    temp7 = "" if (list7[i]) == None else str(list7[i])

                    if temp != "" and temp1 != "" and temp2 != "" and temp3 != "" and temp4 != "" and temp5 != "" and temp6 != "" and temp7 != "":
                        tempStr = '({0},{1},{2},{3},{4},{5},{6},{7},{8}),'.format(
                            "'" + timeList[i].strftime('%Y-%m-%d') + "'",
                            "'" + temp + "'",
                            "'" + temp1 + "'",
                            "'" + temp2 + "'",
                            "'" + temp3 + "'",
                            "'" + temp4 + "'",
                            "'" + temp5 + "'",
                            "'" + temp6 + "'",
                            "'" + temp7 + "'"
                        )
                        appendStr = appendStr + tempStr
                    i += 1

                # 有数据，做插入操作
                if(len(appendStr) != 0):
                    mysql = MySQLDB()
                    mysql.connectMysql()
                    # 写入语句
                    sql = "insert into wind_rzrq(TRADE_DATE,RZMR_MRE,RZMR_CHE,RQMC_MCL,RQMC_CHL,QMYE_RZYE,QMYE_RQYE,QMYE_RQYL,AGSZ) values"
                    insertSql = sql + appendStr[0:len(appendStr) - 1]
                    n = mysql.insert(insertSql)
                    print("写入条数：", n)
                    # 关闭连接
                    mysql.conn.close()


    def saveBatchRzrq():
        '''
        批量写入数据 "600061.SH",
        :return:
        '''
        windCode = "600061.SH"
        startDate = '2019-12-19'
        endDate = '2019-12-20'
        print('融资融券：写入wind编码为',windCode,'的数据，日期范围：',startDate,'~',endDate,':')
        Rzrq.insertBatchRzrq(windCode, startDate, endDate)

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
        dt4 = datetime.now() + timedelta(days=-4)
        dtList = [dt, dt2, dt3, dt4]

        windCode = "600061.SH"
        for dt in dtList:
            print('写入wind编码为', windCode, '的数据，日期：', dt.strftime('%Y-%m-%d'), ':')
            logging.info('写入wind编码为' + windCode + '的数据，日期：' + dt.strftime('%Y-%m-%d') + ':')
            Rzrq.insertSpeDateRzrq(windCode, dt)
            # 写入时间，间隔2秒
            time1.sleep(2)
        print('************************ 写入融资融券数据 end ************************')
        logging.info('************************ 写入融资融券数据 end ************************')

if __name__ == '__main__':
    Rzrq.saveTaskRzrq()
    # 批量写入其他企业数据 近两年
    # Rzrq.saveBatchRzrq()