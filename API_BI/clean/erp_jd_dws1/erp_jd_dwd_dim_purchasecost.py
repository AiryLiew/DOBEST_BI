# -*- coding: utf-8 -*-
# 测试环境: python3.9.6


# *****************************************自定义函数路径*************************************************#
import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

from key_tab import savesql
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine,text
import numpy as np


# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306')) 


df_purchaseOrders = pd.read_sql_query(text("""select year,wlmc_all ,sum(jiashuihj)/sum(caigousl_new) cost from erp_jd_dwd.erp_jd_dwd_dim_purchaseorders group by year,wlmc_all having sum(caigousl_new)<>0;"""),   engine.connect()) 


engine.dispose()   

# ******************************************清洗表*******************************************************#

# 年平均采购成本计算
df_purchasecost = df_purchasecost.pivot(index='wlmc_all', columns='year', values='cost')
df_purchasecost.fillna(method='ffill', axis=1,inplace=True)
df_purchasecost.fillna(method='bfill', axis=1,inplace=True)
df_purchasecost = pd.DataFrame(df_purchasecost.stack())
df_purchasecost.reset_index(inplace=True)
df_purchasecost.rename(columns={0:'cost'},inplace=True)


df_purchasecost['refresh'] = datetime.now()

# *****************************************写入mysql*****************************************************#      

savesql(df_purchasecost,'erp_jd_dwd','erp_jd_dwd_dim_purchasecost',"""CREATE TABLE `erp_jd_dwd_dim_purchasecost` (
  `wlmc_all` text,
  `year` text,
  `cost` double DEFAULT NULL,
  `refresh` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO erp_jd_dwd_dim_purchasecost(wlmc_all,year,cost,refresh) VALUES (%s,%s,%s,%s)")