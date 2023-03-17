import os 
from apscheduler.schedulers.blocking import BlockingScheduler

def job_function():    
    os.system(r"python C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\ods.py")
    os.system(r"python C:\Users\liujin02\Desktop\BI建设\API_BI\clean\api_jd_cwxz_increment.py")
    os.system(r"python C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\dwd.py")
    os.system(r"python C:\Users\liujin02\Desktop\BI建设\API_BI\clean\erp_jd_dwd.py")
    os.system(r"python C:\Users\liujin02\Desktop\BI建设\API_BI\clean\erp_jd_dws.py")
    os.system(r"python C:\Users\liujin02\Desktop\BI建设\API_BI\clean\erp_jd_dws\erp_jd_dws_warehouse_ck_dayend.py")
    os.system(r"python C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\dws.py")
    os.system(r"python C:\Users\liujin02\Desktop\BI建设\API_BI\clean\erp_jd_ads\key_product_doi_fc.py") 
    os.system(r"python C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\ads.py")
    os.system(r"python C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\day.py")
    os.system(r"python C:\Users\liujin02\Desktop\BI建设\API_BI\clean\erp_jd_dws_qd_dlzy.py")
    os.system(r"python C:\Users\liujin02\Desktop\BI建设\API_BI\clean\erp_jd_dws\dlzy_inventory_ck_doi.py")
    os.system(r"python C:\Users\liujin02\Desktop\BI建设\API_BI\clean\erp_jd_ads\erp_jd_ads_ageofreceivables_1122_06.py")
    os.system(r"python C:\Users\liujin02\Desktop\邮件报表\产品首批采购消耗完成时间.py")   
    os.system(r"python C:\Users\liujin02\Desktop\邮件报表\核心桌游_扩展滞销风险.py") 
    os.system(r"python C:\Users\liujin02\Desktop\邮件报表\渠道客户拿货预警.py") 
    os.system(r"python C:\Users\liujin02\Desktop\邮件报表\客户库存留存估计.py") 
    os.system(r"python C:\Users\liujin02\Desktop\邮件报表\西西弗代理加单.py") 
    os.system(r"python C:\Users\liujin02\Desktop\邮件报表\孩子王代理加单.py") 
    os.system(r"python C:\Users\liujin02\Desktop\邮件报表\西西弗自研加单.py") 
    os.system(r"python C:\Users\liujin02\Desktop\邮件报表\孩子王自研加单.py") 
    os.system(r"python C:\Users\liujin02\Desktop\邮件报表\渠道活动返利计算.py")  
    os.system(r"python C:\Users\liujin02\Desktop\邮件报表\毛利比对.py")  
    os.system(r"python C:\Users\liujin02\Desktop\邮件报表\caigou.py") 
    os.system(r"python C:\Users\liujin02\Desktop\邮件报表\product_xxf.py")
    os.system(r"python C:\Users\liujin02\Desktop\邮件报表\wlgzs.py")
    os.system(r"python C:\Users\liujin02\Desktop\BI建设\API_BI\forecast_sales\forecast_sales_wlmc.py")  
    os.system(r"python C:\Users\liujin02\Desktop\邮件报表\spider_bgg.py") 


def job_function2():   
    os.system(r"python C:\Users\liujin02\Desktop\邮件报表\spider_qlwq.py")
    

# def job_function4():   
    # os.system(r"python C:\Users\liujin02\Desktop\邮件报表\spider_hxzy.py")

def job_function5():   
    # os.system(r"python C:\Users\liujin02\Desktop\BI建设\API_BI\clean\api_jd_cwxz_increment_cwbb.py")
    os.system(r"python C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\Friday.py") 
    os.system(r"python C:\Users\liujin02\Desktop\BI建设\API_BI\clean\erp_jd_ads\erp_jd_ads_ageofreceivables_1122_06.py")


def job_function7(): 
    os.system(r"python C:\Users\liujin02\Desktop\邮件报表\渠道客户周销量变化map.py") 

sched = BlockingScheduler()


# sched.add_job(job_function2, 'interval', hours=168, coalesce=True,misfire_grace_time=3600,start_date='2022-10-21 00:00:00', end_date='2025-06-18 18:30:00')
# sched.add_job(job_function, 'interval', hours=24, coalesce=True,misfire_grace_time=3600,start_date='2022-02-24 18:00:00', end_date='2025-06-18 18:30:00')
# sched.add_job(job_function5, 'interval', hours=168, coalesce=True,misfire_grace_time=3600,start_date='2022-07-15 14:20:00', end_date='2025-06-18 18:30:00')
# sched.add_job(job_function7, 'interval', hours=168, coalesce=True,misfire_grace_time=3600,start_date='2022-11-28 00:00:00', end_date='2025-06-18 18:30:00')


sched.add_job(job_function2, 'interval', hours=168, coalesce=True,misfire_grace_time=3600,start_date='2022-10-21 00:00:00', end_date='2025-06-18 18:30:00')
sched.add_job(job_function, 'interval', hours=24, coalesce=True,misfire_grace_time=3600,start_date='2022-02-24 00:00:00', end_date='2025-06-18 18:30:00')
sched.add_job(job_function5, 'interval', hours=168, coalesce=True,misfire_grace_time=3600,start_date='2022-07-15 14:20:00', end_date='2025-06-18 18:30:00')
sched.add_job(job_function7, 'interval', hours=168, coalesce=True,misfire_grace_time=3600,start_date='2022-11-28 03:00:00', end_date='2025-06-18 18:30:00')

sched.start()


# psutil:获取系统信息模块，可以获取CPU，内存，磁盘等的使用情况
# import psutil
# import datetime
# #logfile：监测信息写入文件
# def MonitorSystem(logfile = None):
#     #获取cpu使用情况
#     cpuper = psutil.cpu_percent()
#     #获取内存使用情况：系统内存大小，使用内存，有效内存，内存使用率
#     mem = psutil.virtual_memory()
#     #内存使用率
#     memper = mem.percent
#     #获取当前时间
#     now = datetime.datetime.now()
#     ts = now.strftime('%Y-%m-%d %H:%M:%S')
#     line = f'{ts} cpu:{cpuper}%, mem:{memper}%'
#     print(line)
#     if logfile:
#         logfile.write(line)
# MonitorSystem()