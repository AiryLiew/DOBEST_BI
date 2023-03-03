# -*- coding: utf-8 -*-
# 测试环境: python3.9.6

# day refresh 14:30:00
import pymysql

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


sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_erp_jd_dws_fact_account.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_erp_jd_dws_balance.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_erp_jd_ads_balance_1122_06.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_erp_jd_dws_voucher_merge.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_erp_jd_ads_closebalance.sql')
# sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\erp_jd_dwd\x_wuliaomc_merge.sql')
# sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\erp_jd_ads\x_customer_valid.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_erp_jd_dws_factory_dayend.sql')

c.close()
conn.close()