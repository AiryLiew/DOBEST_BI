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
conn = create_engine("mssql+pymssql://{}:{}@{}:{}/{}".format('sa', '123456', '10.242.21.1', '1433', 'erp_jd_dws'))

        
# *****************************************取数据********************************************************#
df_purchaseorders    = pd.read_sql_query('select * from erp_jd_dwd_dim_purchaseorders;',   engine2)


# ******************************************清洗表*******************************************************#
# df_purchase_contrast  产品采购成本变动表(上年度对比)
# ----------------------------------------------------------------------------------------------------- # 
df_purchase_year = df_purchaseorders[df_purchaseorders['year']==str(datetime.today().year)]
df_purchase_lyear = df_purchaseorders[df_purchaseorders['year']==str(datetime.today().year-1)]
df_purchase_lyear['hanshuidj1'] = df_purchase_lyear['hanshuidj']
A = df_purchase_year.groupby(['wuliaomc','month'])['hanshuidj'].mean().unstack()
B = df_purchase_lyear.groupby('wuliaomc').agg({'hanshuidj':'min','hanshuidj1':'max'})

df_purchase_contrast = B.join(A,how='outer')

data_con = df_purchase_contrast[df_purchase_contrast.columns[2:]]
dict_con = dict(zip(data_con.index ,data_con.values ))

last_value = []
for k, v in dict_con.items():
    output = v[~np.isnan(v)]
    try:
        val = output[-1]
        last_value.append(val)
    except:
        last_value.append('本年未采购')
df_purchase_contrast.reset_index(inplace=True)
df_purchase_contrast['last_value'] = pd.DataFrame(last_value)

list_st = []
for i in range(len(df_purchase_contrast)):
    if df_purchase_contrast['last_value'][i] == '本年未采购':
        list_st.append('本年未采购')
    elif df_purchase_contrast['last_value'][i] >= df_purchase_contrast['hanshuidj'][i] and df_purchase_contrast['last_value'][i] <= df_purchase_contrast['hanshuidj1'][i]:
        list_st.append('持平')
    elif df_purchase_contrast['last_value'][i] < df_purchase_contrast['hanshuidj'][i]:
        list_st.append('下降')
    elif df_purchase_contrast['last_value'][i] > df_purchase_contrast['hanshuidj1'][i]:
        list_st.append('上涨')
    else:
        list_st.append('新品')
df_purchase_contrast['成本变动'] = pd.DataFrame(list_st)


# *****************************************写入mysql*****************************************************#
df_purchase_contrast.to_sql('erp_jd_dws_purchasecontrast',engine1, schema='erp_jd_dws', if_exists='replace',index=False)
engine1.dispose()
engine2.dispose()


# *****************************************写入sql server************************************************#
df_purchase_contrast.to_sql(name='erp_jd_dws_purchasecontrast',con=conn, if_exists='replace',index=False)
conn.dispose() 