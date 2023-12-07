# -*- coding: utf-8 -*-
# 测试环境: python3.9.6


# *****************************************自定义函数路径*************************************************#
import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

from key_tab import merge_label,insertsql,cf
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine,text


# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306')) 


# *****************************************取数据********************************************************#
df_purchaseReturn = pd.read_sql_query(text("""
                                        select * from erp_jd_ods.erp_jd_ods_dim_purchasereturn_wc_cwzx
                                        where tuiliaorq>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_purchasereturn_ms_cwzx
                                        where tuiliaorq>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_purchasereturn_wc01_cwzx
                                        where tuiliaorq>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')                                       

                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_purchasereturn_yc_cwzx
                                        where tuiliaorq>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_purchasereturn_kyk_cwzx
                                        where tuiliaorq>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01');"""), engine.connect()) 




df_wlys = pd.read_sql_query(text('SELECT * FROM `erp_jd_dwd`.`erp_jd_dwd_fact_wuliaomc_ys`;'),   engine.connect())  
df_saleShipping = pd.read_sql_query(text('SELECT DISTINCT wuliaomc FROM `erp_jd_dwd`.`erp_jd_dwd_dim_saleshipping`;'),   engine.connect())  
engine.dispose()   


# ******************************************清洗表*******************************************************#
# df_purchaseReturn   采购退料表
# ----------------------------------------------------------------------------------------------------- #
# 修正物料名称
df_purchaseReturn = cf(df_purchaseReturn)
df_purchaseReturn = merge_label(df_purchaseReturn,df_saleShipping['wuliaomc'].to_list(), 'shituisl','tuiliaorq',df_wlys)

df_purchaseReturn.drop(['refresh_jk','fid','wlmc_new'],axis=1,inplace = True)
df_purchaseReturn['refresh'] = datetime.now()

print('df_purchaseReturn:',datetime.now())

# *****************************************写入mysql*****************************************************#   
insertsql(df_purchaseReturn,
          'erp_jd_dwd',
          'erp_jd_dwd_dim_purchasereturn',
          """INSERT INTO erp_jd_dwd_dim_purchasereturn(tuiliaorq,gongyingsid,gongyingsmc,wuliaobm,wuliaofzid,wuliaofzmc,cangkuid,cangkumc,shituisl,hanshuidj,jiashuihj,danjubh,company,year,month,wuliaomc,wlmc_all,label,shituisl_new,shifoucp,mark_cp,refresh) 
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
		  'tuiliaorq')
