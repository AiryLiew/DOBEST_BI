print(1)
import os 
from apscheduler.schedulers.blocking import BlockingScheduler
def job_function():
    os.system(r"python C:\Users\liujin02\Desktop\邮件报表\spider_keywords.py")

def job_function1():
    os.system(r"python C:\Users\liujin02\Desktop\邮件报表\spider_mail.py")

sched = BlockingScheduler()

# 周期触发的时间范围在2020-06-18 14:23 至 2020-06-18 18:30 ,每两分钟一次
sched.add_job(job_function, 'interval', hours=6,coalesce=True,misfire_grace_time=3600, start_date='2022-02-24 00:00:00', end_date='2024-06-18 18:30:00')
sched.add_job(job_function1, 'interval', hours=24,coalesce=True,misfire_grace_time=3600, start_date='2022-02-24 10:00:00', end_date='2024-06-18 18:30:00')

sched.start()


