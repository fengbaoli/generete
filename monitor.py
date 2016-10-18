# -*- coding: utf-8 -*-
__author__ = '343715'
import urllib2
import json
import  datetime,time


#定义环境变量

oozie_server_address="10.249.35.115"
api_address="http://%s:11000/oozie/v1/jobs?timezone=Asia/Shanghai" %(oozie_server_address)
req = urllib2.Request(api_address)
response = urllib2.urlopen(req)
output = response.read()
json_output= json.loads(output)
workflows_list = json_output["workflows"]
oozie_monitor_file_path="/opt/hadoop/logs/oozie/"
monitor_filename = "oozie_monitor.log"
#告警warning running时间间隔
warnning_time_interval=3
#告警error running时间间隔
error_time_interval=600

import logging
import os


class logdebug:
    def loginfo(self,message):
        self.message = message
        logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename=oozie_monitor_file_path+"monitor.log",
                filemode='w')
        logging.info(self.message)
    def logerror(self,message):
        self.message = message
        logging.basicConfig(level=logging.ERROR,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename=oozie_monitor_file_path+"monitor.log",
                filemode='w')
        logging.error(message)



def get_time_interval(start_datetime):
    #格式化开始时间
    format_datetime = start_datetime.split()[0].strip(",")+" "+start_datetime.split()[1]+" "+start_datetime.split()[2]+" "+start_datetime.split()[3] +" "+start_datetime.split()[4]
    #开始时间转换为datetime类型
    start_time = datetime.datetime.strptime(format_datetime,"%a %d %b %Y %H:%M:%S")
    #格式化现在时间
    format_now_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #将现在时间装换为datetime类型
    now_time=datetime.datetime.strptime(format_now_datetime,'%Y-%m-%d %H:%M:%S')
    #获取开始时间和现在时间之差
    time_interval = now_time - start_time
    #获取开始时间和现在时间的间隔天数
    interval_days = time_interval.days
    #获取开始时间和现在时间的间隔秒数
    interval_seconds= time_interval.seconds
    #间隔总秒数
    total_interval_seconds=interval_days*24*60*60+interval_seconds
    return  total_interval_seconds,start_time


def write_files(appname,log_level,create_time,now_intelval_time):
    if not os.path.exists(oozie_monitor_file_path):
        os.mkdir(oozie_monitor_file_path)
    now_time = format_now_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        files = open(oozie_monitor_file_path+monitor_filename,mode="a")
        message=str(now_time)+"  "+log_level+" "+"Appname:"+appname+" create at:"+create_time+","+\
                appname+ " has running "+str(now_intelval_time) +" seconds,please check job running status!\n"
        files.write(message)
        files.close()
    except IOError as err:
        log = logdebug()
        error_message='File Error:'+str(err)
        log.logerror(error_message)



def files_roll():
    now_time = datetime.datetime.now().strftime('%H:%M:%S')
    now_date= datetime.datetime.now().strftime('%Y-%m-%d')
    Hour = int(now_time.split(":")[0])
    Minute = int(now_time.split(":")[1])
    if Hour == 0 and Minute == 0 or Minute ==1 :
        src_filename = oozie_monitor_file_path+monitor_filename
        dst_filename =oozie_monitor_file_path+monitor_filename+"-"+str(now_date)
        if not os.path.exists(dst_filename) and os.path.exists(src_filename):
            os.rename(src=src_filename,dst=dst_filename)
            os.mknod(src_filename)



def monitor_oozie_job():
    for job_dict in workflows_list:
        status = job_dict["status"]
        #监控文件归档
        files_roll()
        if status == "RUNNING":
            createdTime =job_dict["createdTime"]
            now_intelval_time,start_time=get_time_interval(createdTime)
            #运行正常
            if now_intelval_time >= 0 and now_intelval_time <= warnning_time_interval:
                appName = job_dict["appName"]
                write_files(appname=appName,log_level="INFO",create_time=str(start_time),now_intelval_time=now_intelval_time)
            #运行超过5分钟。WARN
            elif now_intelval_time > warnning_time_interval and now_intelval_time <= error_time_interval:
                appName = job_dict["appName"]
                write_files(appname=appName,log_level="WARN",create_time=str(start_time),now_intelval_time=now_intelval_time)
            #运行超过10分钟。ERROR
            elif now_intelval_time > error_time_interval:
                appName = job_dict["appName"]
                write_files(appname=appName,log_level="ERROR",create_time=str(start_time),now_intelval_time=now_intelval_time)
            else:
                pass
        else:
            pass

if __name__=="__main__":
    monitor_oozie_job()


