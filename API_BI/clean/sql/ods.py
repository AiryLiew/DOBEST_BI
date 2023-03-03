# -*- coding: utf-8 -*-
# 测试环境: python3.9.6

# day refresh 17:50:00
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



sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_erp_jd_ods.sql')

c.close()
conn.close()