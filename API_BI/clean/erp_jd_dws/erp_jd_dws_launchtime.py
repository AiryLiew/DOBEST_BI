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
# conn = create_engine("mssql+pymssql://{}:{}@{}:{}/{}".format('sa', '123456', '10.242.21.1', '1433', 'erp_jd_dws'))

        
# *****************************************取数据********************************************************#
df_purchasereceiving = pd.read_sql_query('select * from erp_jd_dwd_dim_purchasereceiving;',engine2)


# ******************************************清洗表*******************************************************#
# df_launchtime   产品上市时间表
# ----------------------------------------------------------------------------------------------------- # 
df_launchtime = df_purchasereceiving.groupby('wlmc_all',as_index=False)['riqi'].min()


# *****************************************写入mysql*****************************************************#
df_launchtime       .to_sql('erp_jd_dws_launchtime',      engine1, schema='erp_jd_dws', if_exists='replace',index=False)
engine1.dispose()
engine2.dispose()


# # *****************************************写入sql server************************************************#
# df_launchtime       .to_sql(name='erp_jd_dws_launchtime',      con=conn, if_exists='replace',index=False)
# conn.dispose() 