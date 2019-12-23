# -*- coding：utf-8 -*-#

#--------------------------------------------------------------
#NAME:          main
#Description:  获取财务数据
#Author:        xuezy
#Date:          2019/9/12
#--------------------------------------------------------------
from mysqlutil import MySQLDB
import datetime
import time as time1
import logging
from WindPy import *
w.start()


#insertData('2019-09-27', '2019-09-28')
#  600061.SH   1     国投资本
#  882501.WI   2    券商基准（投资银行业与经济业指数）
#  600705.SH   3    中航资本
#  600390.SH   4    五矿资本
#  002423.SZ   5    中粮资本（原中原特钢）
#  000617.SZ   6    中油资本
#  600958.SH   7   东方证券
#  601901.SH   8   方正证券
#  601788.SH   9   光大证券
#  601377.SH   10  兴业证券
#  000001.SH   11  上证综指
# 002736.SZ    12  国信证券

class Cwsj:
    def insertBatchCwsj(windCode, startDate, endDate):
        '''
        导入指定企业、指定日期范围的数据，参数为字符串
        :param windCode: wind编码
        :param startDate: 开始日期，如 '1997-05-19'
        :param endDate: 结束日期，如 '2019-09-12'
        :return:
        '''

        # 获取当前年份
        currentYear = datetime.now().year
        # 拼接wss函数尾部内容
        param = "year=" + str(currentYear)
        '''
        wsd日期序列：可以获取一个时间段内的数据
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
        data = w.wsd(windCode, "windcode,sec_name,open,close,high,low,volume,pct_chg,turn,pe_ttm,pb_lf,pb_mrq,mkt_cap_ard,amt,pe_est,est_eps, west_eps_fy1_1m, west_eps_fy1_3m, west_eps_fy1_6m, est_netprofit, west_netprofit_fy1_1m, west_netprofit_fy1_3m, west_netprofit_fy1_6m", startDate, endDate, param)

        errorCode = data.ErrorCode
        if errorCode != 0:
            if errorCode == -40522017:
                print('从wind获取数据出错，错误代码：', errorCode, '，数据提取量超限')
                logging.error('从wind获取数据出错，错误代码：' + str(errorCode) + '，数据提取量超限')
            else:
                print('从wind获取数据出错，错误代码：', errorCode)
                logging.error('从wind获取数据出错，错误代码：' + str(errorCode))
        else:
            # 日期
            timeList = data.Times
            # 代码
            code = data.Data[0][0]
            # 简称
            list1 = data.Data[1]
            # 开盘价(元)
            list2 = data.Data[2]
            # 收盘价(元)
            list3 = data.Data[3]
            # 最高价(元)
            list4 = data.Data[4]
            # 最低价(元)
            list5 = data.Data[5]
            # 成交量(股)
            list6 = data.Data[6]
            # 涨跌幅(%)
            list7 = data.Data[7]
            # 换手率(%)
            list8 = data.Data[8]
            # 市盈率PE(TTM)
            list9 = data.Data[9]
            # 市净率PB(LF,内地)
            list10 = data.Data[10]
            # 市净率PB(MRQ)
            list11 = data.Data[11]
            # 总市值(元)
            list12 = data.Data[12]
            # 成交额
            list13 = data.Data[13]
            # 预测PE
            list14 = data.Data[14]
            # 预测EPS(平均)
            list15 = data.Data[15]
            # EPS一致预测 - 30天变化率
            list16 = data.Data[16]
            # EPS一致预测-90天变化率
            list17 = data.Data[17]
            # EPS一致预测-180天变化率
            list18 = data.Data[18]
            # 预测归母净利润(平均,单位：元)
            list19 = data.Data[19]
            # 归母净利润一致预测-30天变化率
            list20 = data.Data[20]
            # 归母净利润一致预测-90天变化率
            list21 = data.Data[21]
            # 归母净利润一致预测-180天变化率
            list22 = data.Data[22]


            # 获取数据量
            size = len(timeList)
            # 简称
            shorName = ''
            if len(list1):
                # 投资银行业与经纪业指数 更名为 券商基准
                shorName = list1[0] if code != '882501.WI' else '券商基准'

            if size > 0:
                i = 0
                appendStr = ''
                while i < size:
                    # 开盘价(元)
                    temp2 = "" if (list2[i]) == None else str(list2[i])
                    # 收盘价(元)
                    temp3 = "" if (list3[i]) == None else str(list3[i])
                    # 最高价(元)
                    temp4 = "" if (list4[i]) == None else str(list4[i])
                    # 最低价(元)
                    temp5 = "" if (list5[i]) == None else str(list5[i])
                    # 成交量(股)
                    temp6 = "" if (list6[i]) == None else str(list6[i])
                    # 涨跌幅(%)
                    temp7 = "" if (list7[i]) == None else str(list7[i])
                    # 换手率(%)
                    temp8 = "" if (list8[i]) == None else str(list8[i])
                    # 市盈率PE(TTM)
                    temp9 = "" if (list9[i]) == None else str(list9[i])
                    # 市净率PB(LF,内地)
                    temp10 = "" if (list10[i]) == None else str(list10[i])
                    # 市净率PB(MRQ)
                    temp11 = "" if (list11[i]) == None else str(list11[i])
                    # 总市值(元)
                    temp12 = "" if (list12[i]) == None else str(list12[i])
                    # 成交额
                    temp13 = "" if (list13[i]) == None else str(list13[i])
                    # 预测PE
                    temp14 = "" if (list14[i]) == None else str(list14[i])
                    # 预测EPS(平均)
                    temp15 = "" if (list15[i]) == None else str(list15[i])
                    # EPS一致预测 - 30天变化率
                    temp16 = "" if (list16[i]) == None else str(list16[i])
                    # EPS一致预测-90天变化率
                    temp17 = "" if (list17[i]) == None else str(list17[i])
                    # EPS一致预测-180天变化率
                    temp18 = "" if (list18[i]) == None else str(list18[i])
                    # 预测归母净利润(平均,单位：元)
                    temp19 = "" if (list19[i]) == None else str(list19[i])
                    # 归母净利润一致预测-30天变化率
                    temp20 = "" if (list20[i]) == None else str(list20[i])
                    # 归母净利润一致预测-90天变化率
                    temp21 = "" if (list21[i]) == None else str(list21[i])
                    # 归母净利润一致预测-180天变化率
                    temp22 = "" if (list22[i]) == None else str(list22[i])

                    # 市净率PB(MRQ) 和 总市值(元) 之外的属性 均不为空 写入该条数据
                    if temp2 != "" and temp3 != "" and temp4 != "" and temp5 != "" and temp6 != "" and temp7 != "" and temp8 != "" and temp9 != "" and temp10 != "" and temp13 != "" :
                        tempStr = '({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23}),'.format(
                            "'" + code + "'",
                            "'" + shorName + "'",
                            "'" + timeList[i].strftime('%Y-%m-%d') + "'",
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
                        appendStr = appendStr + tempStr
                    i += 1

                # 有数据，做插入操作
                if(len(appendStr) != 0):
                    mysql = MySQLDB()
                    mysql.connectMysql()
                    # 写入语句
                    sql = "insert into wind_jrjyl(CODE,SHORT_NAME,TRADE_DATE,OPEN_PRICE,CLOSE_PRICE,MAX_PRICE,MIN_PRICE,VOL,CHG,TURNOVER_RATE,PE_TTM,PB_LF,PB_MRQ,EV,AMT,PE_EST,EST_EPS,EPS_1M,EPS_3M,EPS_6M,EST_NETPROFIT,NETPROFIT_1M,NETPROFIT_3M,NETPROFIT_6M) values"
                    insertSql = sql + appendStr[0:len(appendStr) - 1]
                    n = mysql.insert(insertSql)
                    print("写入条数：", n)
                    # 关闭连接
                    mysql.conn.close()


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

    #  600061.SH   1     国投资本
    #  882501.WI   2    券商基准（投资银行业与经济业指数）
    #  600705.SH   3    中航资本
    #  600390.SH   4    五矿资本
    #  002423.SZ   5    中粮资本（原中原特钢）
    #  000617.SZ   6    中油资本
    #  600958.SH   7   东方证券
    #  601901.SH   8   方正证券
    #  601788.SH   9   光大证券
    #  601377.SH   10  兴业证券
    #  000001.SH   11  上证综指
    # 002736.SZ    12  国信证券
    def saveBatchCwsj():
        '''
        批量写入数据
        :return:
        '''
        windCode = ["600061.SH","882501.WI", "600705.SH", "600390.SH", "002423.SZ", "000617.SZ", "600958.SH", "601901.SH", "601788.SH", "601377.SH", "000001.SH", "002736.SZ"]
        startDate = '2019-12-18'
        endDate = '2019-12-20'
        for item in windCode:
            print('写入wind编码为',item,'的数据，日期范围：',startDate,'~',endDate,':')
            Cwsj.insertBatchCwsj(item, startDate, endDate)
            # 写入时间，间隔1秒
            time1.sleep(2)

    def saveTaskCwsj():
        '''
        批量写入指定日期数据
        :return:
        '''
        # Logger.make_print_to_file(path="./log/")
        print('************************ 写入安信证券财务数据 start ************************')
        logging.info('************************ 写入安信证券财务数据 start ************************')
        # 获取前5天时间，有不写入，无则写入
        dt = datetime.now() + timedelta(days=-1)
        dt2 = datetime.now() + timedelta(days=-2)
        dt3 = datetime.now() + timedelta(days=-3)
        dt4 = datetime.now() + timedelta(days=-4)
        dt5 = datetime.now() + timedelta(days=-5)
        dtList = [dt, dt2, dt3, dt4, dt5]

        windCode = ["600061.SH", "882501.WI", "600705.SH", "600390.SH", "002423.SZ", "000617.SZ", "600958.SH", "601901.SH", "601788.SH", "601377.SH", "000001.SH", "002736.SZ"]
        for item in windCode:
            for dt in dtList:
                print('写入wind编码为', item, '的数据，日期：', dt.strftime('%Y-%m-%d'), ':')
                logging.info('写入wind编码为' + item + '的数据，日期：' + dt.strftime('%Y-%m-%d') + ':')
                Cwsj.insertSpeDateCwsj(item, dt)
                # 写入时间，间隔2秒
                time1.sleep(2)
        print('************************ 写入安信证券财务数据 end ************************')
        logging.info('************************ 写入安信证券财务数据 end ************************')
if __name__ == '__main__':
    Cwsj.saveTaskCwsj()
    # 批量写入其他企业数据 近两年
    # Cwsj.saveBatchCwsj()
    # 写入国投资本数据 2009-01-01 以来
    # Cwsj.insertBatchCwsj("600061.SH", '2009-01-01', '2013-12-31')