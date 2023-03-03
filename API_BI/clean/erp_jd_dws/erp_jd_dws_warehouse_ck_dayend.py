# -*- coding: utf-8 -*-
# 测试环境: python3.9.6


# *****************************************自定义函数路径*************************************************#
import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

import pandas as pd
from sqlalchemy import create_engine,text
import pymysql
from datetime import datetime , timedelta

# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306')) 
        
# *****************************************取数据********************************************************#
df_warehouse   = pd.read_sql_query(text('select * from erp_jd_dws.erp_jd_dws_warehouse;'), engine.connect()) 
dlzy_inventory = pd.read_sql_query(text('select * from www_bi_ads.dlzy_inventory;'), engine.connect()) 


engine.dispose()


# 创建日期辅助表
def create_assist_date(datestart,dateend):
    date_list = []
    date_list.append(datestart)
    while datestart < dateend:
        datestart+=timedelta(days=+1)
        date_list.append(datestart)
    return pd.DataFrame(date_list,columns=['riqi'])




# df_warehouse_dayend  日库存结余（含月末）
# ----------------------------------------------------------------------------------------------------- # 
def dayend(df_warehouse):
    df_warehouse_dayend = df_warehouse.groupby(['wuliaomc','cangkumc','riqi'],as_index=False).agg({'receiving':'sum','shipping':'sum'})
    df_warehouse_dayend['inventory'] = df_warehouse_dayend['receiving']-df_warehouse_dayend['shipping']

    def funckc(df_warehouse,name):    
        df_warehouse.sort_values(['wuliaomc','cangkumc','riqi'],ascending=True,inplace=True,ignore_index=True)
        listWL = []
        for i in df_warehouse['wuliaomc'].drop_duplicates():
            dfW = df_warehouse[df_warehouse['wuliaomc'] == i]
            for j in dfW['cangkumc'].drop_duplicates():
                dfW1 = dfW[dfW['cangkumc'] == j]
                dfW1.loc[:,name] = dfW1['inventory'].cumsum()
                listWL.append(dfW1)
        return pd.concat(listWL,ignore_index=True)

    df_warehouse_dayend = funckc(df_warehouse_dayend,'inventory_wl')

    # df_warehouse_dayend.sort_values(['wuliaomc','cangkumc','riqi'],ascending=True,inplace=True,ignore_index=True)
    # df_warehouse_dayend1 = df_warehouse_dayend.set_index(['cangkumc','riqi','wuliaomc']).groupby(['riqi','wuliaomc'])['inventory'].cumsum().reset_index().rename(columns = {'inventory':'inventory_wl'})
    # df_warehouse_dayend = pd.concat([df_warehouse_dayend,df_warehouse_dayend1['inventory_wl']],axis=1)


    df_warehouse_dayend['riqi'] = pd.to_datetime(df_warehouse_dayend['riqi'],format = '%Y-%m-%d')
    # 插入日期
    listFz = []
    dateMax = df_warehouse_dayend['riqi'].max()
    for i in df_warehouse_dayend['wuliaomc'].drop_duplicates():
        dfMid = df_warehouse_dayend[df_warehouse_dayend['wuliaomc']==i]
        for j in dfMid['cangkumc'].drop_duplicates():
            dfMid1 = dfMid[dfMid['cangkumc'] == j]
            dateMin = dfMid1['riqi'].min()
            dateFz = create_assist_date(dateMin,dateMax)
            dfMid1 = pd.merge(dateFz,dfMid1,on =['riqi'],how = 'left')
            dfMid1['receiving'].fillna(0,inplace=True)
            dfMid1['shipping'].fillna(0,inplace=True)
            dfMid1['inventory'].fillna(0,inplace=True)
            dfMid1.fillna(method='ffill',inplace=True)	
            listFz.append(dfMid1)
    df_warehouse_dayend = pd.concat(listFz,ignore_index=True)


    df_warehouse_dayend['year_month'] = df_warehouse_dayend['riqi'].map(lambda x :str(x)[:7])
    dfindex = df_warehouse_dayend[['wuliaomc','cangkumc','year_month']].drop_duplicates(keep='last')
    dfindex.loc[:,'ismonthend'] = '是'
    df_warehouse_dayend = df_warehouse_dayend.join(dfindex['ismonthend'])

    return df_warehouse_dayend

df_warehouse_dayend = dayend(df_warehouse)
dlzy_inventory_dayend= dayend(dlzy_inventory)

# *************************************************************************************************#
df_warehouse_dayend .to_sql('erp_jd_dws_warehouse_ck_dayend',engine, schema='erp_jd_dws', if_exists='replace',index=False)
dlzy_inventory_dayend .to_sql('dlzy_inventory_ck_dayend',engine, schema='www_bi_ads', if_exists='replace',index=False)


# savesql(df_warehouse_dayend,'erp_jd_dws','erp_jd_dws_warehouse_ck_dayend',"""CREATE TABLE `erp_jd_dws_warehouse_ck_dayend` (
#   `riqi` datetime DEFAULT NULL,
#   `wuliaomc` text,
#   `cangkumc` text,
#   `receiving` double DEFAULT NULL,
#   `shipping` double DEFAULT NULL,
#   `inventory` double DEFAULT NULL,
#   `inventory_wl` double DEFAULT NULL,
#   `year_month` text,
#   `ismonthend` text
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
# "INSERT INTO erp_jd_dws_warehouse_ck_dayend(riqi,wuliaomc,cangkumc,receiving,shipping,inventory,inventory_wl,`year_month`,ismonthend) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)")


