# -*- coding: utf-8 -*-
# 测试环境: python3.9.6


# *****************************************自定义函数路径*************************************************#
import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

import pandas as pd
from sqlalchemy import create_engine,text
from datetime import datetime , timedelta
from key_tab import savesql

# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306')) 
        
# *****************************************取数据********************************************************#
df_warehouse  = pd.read_sql_query(text('select * from erp_jd_dws.erp_jd_dws_warehouse;'), engine.connect())  
dlzy_inventory = pd.read_sql_query(text('select * from www_bi_ads.dlzy_inventory;'), engine.connect())  


# 创建日期辅助表
# def create_assist_date(datestart,dateend):
#     a = pd.date_range(start=datestart,end=dateend)
#     return pd.DataFrame(a,columns=['riqi'])




# df_warehouse_dayend  日库存结余（含月末）
# ----------------------------------------------------------------------------------------------------- # 
def dayend(df_warehouse):
    df_warehouse_dayend = df_warehouse.groupby(['wuliaomc','riqi'],as_index=False).agg({'receiving':'sum','shipping':'sum'})
    df_warehouse_dayend['inventory'] = df_warehouse_dayend['receiving']-df_warehouse_dayend['shipping']

    def funckc(df_warehouse,name):    
        df_warehouse.sort_values(['wuliaomc','riqi'],ascending=True,inplace=True,ignore_index=True)
        listWL = []
        for i in df_warehouse['wuliaomc'].drop_duplicates():
            dfW = df_warehouse[df_warehouse['wuliaomc'] == i]
            dfW.loc[:,name] = dfW['inventory'].cumsum()
            listWL.append(dfW)
        return pd.concat(listWL,ignore_index=True)

    df_warehouse_dayend = funckc(df_warehouse_dayend,'inventory_wl')


    df_warehouse_dayend['riqi'] = pd.to_datetime(df_warehouse_dayend['riqi'],format = '%Y-%m-%d')
    # 插入日期
    dateMax = df_warehouse_dayend['riqi'].max()
    
    df_warehouse_dayend = df_warehouse_dayend.join(df_warehouse_dayend.groupby(['wuliaomc'],as_index=False)['riqi'].rank(ascending=False).rename(columns={'riqi':'rank'}))
    a = df_warehouse_dayend[(df_warehouse_dayend['rank']==1)&(df_warehouse_dayend['inventory_wl']>0)&(df_warehouse_dayend['riqi']!=dateMax)]
    a.loc[:,'riqi'] = dateMax
    df_warehouse_dayend = pd.concat([df_warehouse_dayend,a],ignore_index=True)
    df_warehouse_dayend.drop(['rank'],axis=1,inplace=True)


    # listFz = []
    # for i in df_warehouse_dayend['wuliaomc'].drop_duplicates():
    #     dfMid = df_warehouse_dayend[df_warehouse_dayend['wuliaomc']==i]
    #     dateMin = dfMid['riqi'].min()
    #     dateFz = create_assist_date(dateMin,dateMax)
    #     dfMid = pd.merge(dateFz,dfMid,on =['riqi'],how = 'left')
    #     dfMid['receiving'].fillna(0,inplace=True)
    #     dfMid['shipping'].fillna(0,inplace=True)
    #     dfMid['inventory'].fillna(0,inplace=True)
    #     dfMid.fillna(method='ffill',inplace=True)	
    #     listFz.append(dfMid)
    # df_warehouse_dayend = pd.concat(listFz,ignore_index=True)


    df_warehouse_dayend['year_month'] = df_warehouse_dayend['riqi'].map(lambda x :str(x)[:7])
    dfindex = df_warehouse_dayend[['wuliaomc','year_month']].drop_duplicates(keep='last')
    dfindex.loc[:,'ismonthend'] = '是'
    df_warehouse_dayend = df_warehouse_dayend.join(dfindex['ismonthend'])

    return df_warehouse_dayend




# df_warehouse_ck_dayend  日库存结余（含月末）
# ----------------------------------------------------------------------------------------------------- # 
def dayend_ck(df_warehouse):
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



    df_warehouse_dayend['riqi'] = pd.to_datetime(df_warehouse_dayend['riqi'],format = '%Y-%m-%d')
    # 插入日期
    dateMax = df_warehouse_dayend['riqi'].max()

    df_warehouse_dayend = df_warehouse_dayend.join(df_warehouse_dayend.groupby(['wuliaomc','cangkumc'],as_index=False)['riqi'].rank(ascending=False).rename(columns={'riqi':'rank'}))
    a = df_warehouse_dayend[(df_warehouse_dayend['rank']==1)&(df_warehouse_dayend['inventory_wl']>0)&(df_warehouse_dayend['riqi']!=dateMax)]
    a.loc[:,'riqi'] = dateMax
    df_warehouse_dayend = pd.concat([df_warehouse_dayend,a],ignore_index=True)
    df_warehouse_dayend.drop(['rank'],axis=1,inplace=True)

    # listFz = []
    # for i in df_warehouse_dayend['wuliaomc'].drop_duplicates():
    #     dfMid = df_warehouse_dayend[df_warehouse_dayend['wuliaomc']==i]
    #     for j in dfMid['cangkumc'].drop_duplicates():
    #         dfMid1 = dfMid[dfMid['cangkumc'] == j]
    #         dateMin = dfMid1['riqi'].min()
    #         dateFz = create_assist_date(dateMin,dateMax)
    #         dfMid1 = pd.merge(dateFz,dfMid1,on =['riqi'],how = 'left')
    #         dfMid1['receiving'].fillna(0,inplace=True)
    #         dfMid1['shipping'].fillna(0,inplace=True)
    #         dfMid1['inventory'].fillna(0,inplace=True)
    #         dfMid1.fillna(method='ffill',inplace=True)	
    #         listFz.append(dfMid1)
    # df_warehouse_dayend = pd.concat(listFz,ignore_index=True)


    df_warehouse_dayend['year_month'] = df_warehouse_dayend['riqi'].map(lambda x :str(x)[:7])
    dfindex = df_warehouse_dayend[['wuliaomc','cangkumc','year_month']].drop_duplicates(keep='last')
    dfindex.loc[:,'ismonthend'] = '是'
    df_warehouse_dayend = df_warehouse_dayend.join(dfindex['ismonthend'])

    return df_warehouse_dayend


# 调用

df_warehouse_ck_dayend = dayend_ck(df_warehouse)
dlzy_inventory_ck_dayend= dayend_ck(dlzy_inventory)



df_warehouse_dayend = dayend(df_warehouse)
dlzy_inventory_dayend = dayend(dlzy_inventory)
dlzy_inventory_dayend = pd.merge(dlzy_inventory_dayend,dlzy_inventory[['属性','wuliaomc']].drop_duplicates(),on=['wuliaomc'],how='left')




# *************************************************************************************************

savesql(df_warehouse_ck_dayend,'erp_jd_dws','erp_jd_dws_warehouse_ck_dayend',"""CREATE TABLE `erp_jd_dws_warehouse_ck_dayend` (
  `wuliaomc` text,
  `cangkumc` text,
  `riqi` datetime DEFAULT NULL,
  `receiving` double DEFAULT NULL,
  `shipping` double DEFAULT NULL,
  `inventory` double DEFAULT NULL,
  `inventory_wl` double DEFAULT NULL,
  `year_month` text,
  `ismonthend` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO erp_jd_dws_warehouse_ck_dayend(wuliaomc,cangkumc,riqi,receiving,shipping,inventory,inventory_wl,`year_month`,`ismonthend`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)")


savesql(df_warehouse_dayend,'erp_jd_dws','erp_jd_dws_warehouse_dayend',"""CREATE TABLE `erp_jd_dws_warehouse_dayend` (
  `wuliaomc` text,
  `riqi` datetime DEFAULT NULL,
  `receiving` double DEFAULT NULL,
  `shipping` double DEFAULT NULL,
  `inventory` double DEFAULT NULL,
  `inventory_wl` double DEFAULT NULL,
  `year_month` text,
  `ismonthend` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO erp_jd_dws_warehouse_dayend(wuliaomc,riqi,receiving,shipping,inventory,inventory_wl,`year_month`,`ismonthend`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)")




savesql(dlzy_inventory_dayend,'www_bi_ads','dlzy_inventory_dayend',"""CREATE TABLE `dlzy_inventory_dayend` (
  `wuliaomc` text,
  `riqi` datetime DEFAULT NULL,
  `receiving` double DEFAULT NULL,
  `shipping` double DEFAULT NULL,
  `inventory` double DEFAULT NULL,
  `inventory_wl` double DEFAULT NULL,
  `year_month` text,
  `ismonthend` text,
  `属性` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO dlzy_inventory_dayend(wuliaomc,riqi,receiving,shipping,inventory,inventory_wl,`year_month`,`ismonthend`,`属性`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)")


savesql(dlzy_inventory_ck_dayend,'www_bi_ads','dlzy_inventory_ck_dayend',"""CREATE TABLE `dlzy_inventory_ck_dayend` (
  `wuliaomc` text,
  `cangkumc` text,
  `riqi` datetime DEFAULT NULL,
  `receiving` double DEFAULT NULL,
  `shipping` double DEFAULT NULL,
  `inventory` double DEFAULT NULL,
  `inventory_wl` double DEFAULT NULL,
  `year_month` text,
  `ismonthend` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO dlzy_inventory_ck_dayend(wuliaomc,cangkumc,riqi,receiving,shipping,inventory,inventory_wl,`year_month`,`ismonthend`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)")