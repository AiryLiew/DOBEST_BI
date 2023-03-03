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
engine3 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'bi')) 
conn = create_engine("mssql+pymssql://{}:{}@{}:{}/{}".format('sa', '123456', '10.242.21.1', '1433', 'erp_jd_dws'))
conn1 = create_engine("mssql+pymssql://{}:{}@{}:{}/{}".format('sa', '123456', '10.242.21.1', '1433', 'bi'))

        
# *****************************************取数据********************************************************#
df_saleshipping = pd.read_sql_query('select * from erp_jd_dwd_dim_saleshipping;', engine2)
df_salereturn   = pd.read_sql_query('select * from erp_jd_dwd_dim_salereturn;',   engine2)
df_launchtime   = pd.read_sql_query('select * from erp_jd_dws_launchtime;',       engine1)


# ******************************************清洗表*******************************************************#
# df_saleship_return   销售出库退货连接表
# ----------------------------------------------------------------------------------------------------- # 
df_salereturn['shifasl'] = - df_salereturn['shifasl']
df_salereturn['jiashuihj'] = - df_salereturn['jiashuihj']
df_salereturn['purchases'] = - df_salereturn['purchases']
df_salereturn['profit'] = - df_salereturn['profit']
df_saleship_return = pd.concat([df_saleshipping,df_salereturn],ignore_index=True)
df_saleship_return = func(df_launchtime,df_saleship_return,'shifasl')
df_saleship_return = level(df_saleship_return)

# 帕累托分析
df_ABC = df_saleship_return.groupby('wuliaofzmc',as_index =False)['jiashuihj'].sum()
df_ABC.sort_values(['jiashuihj'],ascending=False,inplace=True)
suma = df_ABC['jiashuihj'].sum()
df_ABC['累计销售额'] = df_ABC['jiashuihj'].cumsum()
df_ABC['占比'] = df_ABC['jiashuihj'].map(lambda x:x/suma )
df_ABC['累计占比'] = df_ABC['占比'].cumsum()
df_ABC.reset_index(drop=True,inplace=True)


# *****************************************写入mysql*****************************************************#
df_saleship_return.to_sql('erp_jd_dws_saleship_return', engine1, schema='erp_jd_dws', if_exists='replace',index=False)
df_ABC.to_sql('bi_abc', engine3, schema='bi', if_exists='replace',index=False)
engine1.dispose()
engine2.dispose()
engine3.dispose()

# *****************************************写入sql server**********************************************
df_saleship_return.to_sql(name='erp_jd_dws_saleship_return', con=conn, if_exists='replace',index=False)
df_ABC.to_sql(name='bi_abc', con=conn1, if_exists='replace',index=False)
conn.dispose() 
conn1.dispose() 