# -*- coding: utf-8 -*-
# 测试环境: python3.9.6


# *****************************************自定义函数路径*************************************************#
import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

from key_tab import insertsql,getDictKey,clean,cf
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine,text


# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306')) 


# *****************************************取数据********************************************************#
df_cost = pd.read_sql_query(text('SELECT * FROM `erp_jd_dwd`.`erp_jd_dwd_dim_cost`;'), engine.connect())

df_consignment = pd.read_sql_query(text("""
                                        select * from erp_jd_ods.erp_jd_ods_dim_consignment_wc_cwzx 
                                        where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01') and danjubh <> " "
                                        
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_consignment_ms_cwzx 
                                        where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01') and danjubh <> " "
                                        
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_consignment_kyk_cwzx 
                                        where DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01') and danjubh <> " ";"""), engine.connect())


engine.dispose()   


# df_consignment  寄售订单表
# ----------------------------------------------------------------------------------------------------- #
df_consignment.drop(['refresh_jk','fid'],axis=1,inplace = True)

# 
df_consignment = clean(df_consignment,df_cost,'xiaoshoubmmc','hangbencijssl')

# 修正物料名称
df_consignment = cf(df_consignment)

df_consignment['refresh'] = datetime.now()


# *****************************************写入mysql*****************************************************# 
insertsql(	df_consignment,
			'erp_jd_dwd',
			'erp_jd_dwd_dim_consignment',
			"""INSERT INTO erp_jd_dwd_dim_consignment(riqi,kehuid,kehumc,xiaoshoubmdm,xiaoshoubmmc,danjulxdm,danjulxmc,wuliaobm,wuliaomc,jiashuihj,shijijshsdj,hangbencijssl,shouhuofdz,yuandiaorckid,yuandiaorckmc,yuandiaocckid,yuandiaocckmc,danjubh,company,bumen_new,bumen,year,month,cost,purchases,profit,refresh) 
			VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
			'riqi')
