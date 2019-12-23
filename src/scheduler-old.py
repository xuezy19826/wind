# -*- coding：utf-8 -*-#

#--------------------------------------------------------------
#NAME:          main5
#Description:
#Author:        xuezy
#Date:          2019/9/20
#--------------------------------------------------------------
#非阻塞方式
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import time as time1
from mysqlutil import MySQLDB
# 引入获取金融财务数据类
from cwsj import Cwsj
# 引入获取大宗交易（国投资本）数据类
from dzjy import Dzjy
# 引入获取上市公司业绩指标（国投资本）数据类
from yjzb import Yjzb
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
import logging
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from WindPy import *
w.start()

# 配置日志记录信息，日志文件在当前路径，文件名为 “log1.txt”
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

if __name__ == '__main__':
    scheduler = BackgroundScheduler()

    # 添加监听
    scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    # 安信证券-财务报表
    #每天8:15、20：15各执行一次（获取近五天的数据，有数据了不再写入）;有了这个jitter参数，它会使每个任务提前或延后+30/-30秒范围去运行
    cwsj_trigger = CronTrigger(hour='14', minute='40', jitter=30)
    cwsj_trigger2 = CronTrigger(hour='14', minute='45', jitter=30)
    scheduler.add_job(Cwsj.saveTaskCwsj, cwsj_trigger)
    scheduler.add_job(Cwsj.saveTaskCwsj, cwsj_trigger2)

    # 国投资本-大宗交易（获取近2天的数据）
    # dzjy_trigger = CronTrigger(hour='20', minute='20', jitter=30)
    # scheduler.add_job(Dzjy.saveTaskDzjy, dzjy_trigger)

    # 国投资本-上市公司业绩指标（获取近两个季度的数据）
    # yjzb_trigger = CronTrigger(hour='20', minute='25', jitter=30)
    # scheduler.add_job(Yjzb.saveTaskYjzb, yjzb_trigger)

    # 启用 scheduler 模块的日记记录
    scheduler._logger = logging

    # 该部分调度是一个独立的线程
    scheduler.start()

    try:
        # 模拟主进程持续运行，每一天打印一次 86400
        while True:
            time1.sleep(24 * 60 * 60)
            print ('currenttime：',datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    except(KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print('Exit The Job!')