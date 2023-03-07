# -*- coding: utf-8 -*-
# 测试环境: python3.9.6

# Friday refresh 14:33:00
import pymysql
import os 

conn  = pymysql.connect(host="localhost",port = 3306, user="root", password="123456",charset="utf8")
c = conn.cursor()


def sqlrun(path):
    with open(path, 'r', True, 'UTF-8') as f:
        sql = f.read()
        sql = sql.replace('\n' , ' ').replace('\t' , ' ')
        for i in sql.split(';'):
            try:
                c.execute(i)  
            except:
                pass 
    conn.commit()

# 删部分数据
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_erp_jd_ods_dim_voucher_cwzx.sql')
# 接口取数
os.system(r"python C:\Users\liujin02\Desktop\BI建设\API_BI\clean\api_jd_cwxz_increment_cwbb.py")
# 刷新
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_erp_jd_dwd_cwbb.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_erp_jd_dws_fact_account.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_erp_jd_dws_balance.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_erp_jd_ads_balance_1122_06.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_erp_jd_dws_voucher_merge.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_erp_jd_dws_acctagebalance.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_erp_jd_ads_closebalance.sql')

c.close()
conn.close()