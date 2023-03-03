# -*- coding: utf-8 -*-
# 测试环境: python3.9.6


# *****************************************自定义函数路径*************************************************#
import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime 
from key_tab import *


# *****************************************连接mysql、sql server*****************************************#
engine1 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'erp_jd_dws'))
engine2 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'erp_jd_dwd')) 
engine3 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'baidu_map' )) 
conn = create_engine("mssql+pymssql://{}:{}@{}:{}/{}".format('sa', '123456', '10.242.21.1', '1433', 'erp_jd_dws'))

        
# *****************************************取数据********************************************************#
df_saleorders  = pd.read_sql_query('select * from erp_jd_dwd_dim_saleorders;',  engine2)
df_launchtime  = pd.read_sql_query('select * from erp_jd_dws_launchtime;',      engine1)
df_consignment = pd.read_sql_query('select * from erp_jd_dwd_dim_consignment;', engine2)
township_area  = pd.read_sql_query('select * from township_area;',              engine3)


# ******************************************清洗表*******************************************************#
# df_saleorders_qd   渠道销售表
# ----------------------------------------------------------------------------------------------------- # 
sa = df_saleorders[['riqi', 'kehuid', 'kehumc', 'xiaoshoubmdm', 'xiaoshoubmmc',
        'danjulxdm', 'danjulxmc', 'wuliaobm', 'wuliaomc', 'jiashuihj','hanshuidj',
        'xiaoshousl', 'shouhuofdz', 'leijithslxs','cost','bumen_new',
        'bumen','return_am','jiashuihj_ac','xiaoshousl_ac','purchases_ac','profit_ac','danjubh']]

sb = df_consignment[['riqi', 'kehuid', 'kehumc', 'xiaoshoubmdm', 'xiaoshoubmmc',
       'danjulxdm', 'danjulxmc', 'wuliaobm', 'wuliaomc', 'jiashuihj',
       'shijijshsdj', 'hangbencijssl', 'shouhuofdz','cost','purchases','profit','bumen_new','bumen','danjubh']]

sa = sa[sa['danjulxmc'] != '寄售销售订单']
sb.rename(columns={'shijijshsdj':'hanshuidj','hangbencijssl':'xiaoshousl','purchases':'purchases_ac','profit':'profit_ac'},inplace=True)
sb['jiashuihj_ac'] = sb['jiashuihj'] 
sb['xiaoshousl_ac'] = sb['xiaoshousl'] 

df_saleorders_qd = pd.concat([sa,sb],ignore_index=True)
df_saleorders_qd.fillna(0,inplace=True)
df_saleorders_qd['riqi'] = df_saleorders_qd['riqi'].map(lambda x:str(x)[:10])
df_saleorders_qd['year'] = df_saleorders_qd['riqi'].map(lambda x:str(x)[:4])
df_saleorders_qd['month'] = df_saleorders_qd['riqi'].map(lambda x:str(x)[5:7])
df_saleorders_qd = df_saleorders_qd[df_saleorders_qd['wuliaomc']!='代收运费']
# 增加新品判断
list_kh = df_saleorders[['wuliaomc','state_xp']].drop_duplicates()
df_saleorders_qd = pd.merge(df_saleorders_qd,list_kh,on = ['wuliaomc'],how='left')
# 增加客户判断
list_kh = df_saleorders[['kehumc','state_kh']].drop_duplicates()
df_saleorders_qd = pd.merge(df_saleorders_qd,list_kh,on = ['kehumc'],how='left')
# 增加省市等
df_saleorders_qd = area(township_area,df_saleorders_qd)
df_saleorders_qd = func(df_launchtime,df_saleorders_qd,'xiaoshousl')
df_saleorders_qd = level(df_saleorders_qd)


# *****************************************写入mysql*****************************************************#
df_saleorders_qd.to_sql('erp_jd_dws_saleordersqd',    engine1, schema='erp_jd_dws', if_exists='replace',index=False)
engine1.dispose()
engine2.dispose()
engine3.dispose()


# *****************************************写入sql server************************************************#
df_saleorders_qd.to_sql(name='erp_jd_dws_saleordersqd',    con=conn, if_exists='replace',index=False)
conn.dispose() 