# -*- coding: utf-8 -*-
# 测试环境: python3.9.6


# *****************************************自定义函数路径*************************************************#
import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

import pandas as pd
from datetime import datetime ,timedelta
from sqlalchemy import create_engine,text
from key_tab import savesql,getDictKey1,getDict


# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306'))

        
# *****************************************取数据********************************************************#
df_warehouse         = pd.read_sql_query(text('select * from erp_jd_dws.erp_jd_dws_warehouse;'), engine.connect())
df_purchasereceiving = pd.read_sql_query(text('select * from erp_jd_dwd.erp_jd_dwd_dim_purchasereceiving;'), engine.connect())
df_beginninginventory= pd.read_sql_query(text('select * from erp_jd_dwd.erp_jd_dwd_dim_beginninginventory;'), engine.connect())


engine.dispose()

# ******************************************清洗表*******************************************************#
# df_doi   库存天数表
# ----------------------------------------------------------------------------------------------------- # 
def funcA(df_warehouse,df_purchasereceiving,date):  
    a = df_warehouse.groupby('wuliaomc',as_index=False)['inventory'].sum()
    df_purchasereceiving['shifasl_new'] = df_purchasereceiving['shifasl_new'].astype(float)

    b = df_purchasereceiving.groupby(['wlmc_all','riqi'],as_index=False)['shifasl_new'].sum()
    b = b[b['shifasl_new']!= 0]

    c = df_beginninginventory[['wuliaomc','riqi','receiving']]
    c.rename(columns={'receiving':'amount'},inplace=True)
    b.rename(columns={'wlmc_all':'wuliaomc','shifasl_new':'amount'},inplace=True)
    d = pd.concat([c,b],ignore_index=True)


    
    d['amount'] = d['amount'].astype(int)
    d['riqi'] = d['riqi'].map(lambda x:str(x)[:10])
    d['riqi'] = pd.to_datetime(d['riqi'],format='%Y-%m-%d')

    # 增加库存 
    mydict = dict(zip(a['wuliaomc'],a['inventory']))
    list_ = []         
    for i in d['wuliaomc'].drop_duplicates():
        b1 = d[d['wuliaomc']==i]
        b1.sort_values(['riqi'],ascending=False,inplace=True,ignore_index=True)
        b1.reset_index(drop = True,inplace = True)
        # 累计采购及期初数量
        b1.loc[:,'t_amount'] = b1['amount'].cumsum()

        for j in range(len(b1)):
            if getDictKey1(mydict,b1['wuliaomc'][j],0)<=b1['t_amount'][j]:
                list_.append(b1.loc[:j])
                break
    df1 = pd.concat(list_,ignore_index=True)

    df2 = pd.merge(df1,a,on=['wuliaomc'],how='left')
    df2['inventory'].fillna(0,inplace = True)
    df2.dropna(subset=['wuliaomc'],inplace=True)

    df2['surplus'] = pd.Series()
    list_js = []
    for i in df2['wuliaomc'].drop_duplicates():
        b2 = df2[df2['wuliaomc']==i]
        b2.reset_index(drop = True,inplace = True)
        if len(b2)==1:
            b2['surplus'].fillna(b2['inventory'],inplace = True)
            list_js.append(b2)
        else: 
            b2['surplus'].fillna(b2['amount'],inplace = True)
            b2.loc[b2.index[-1],'surplus'] = b2['inventory'][-1:].values[0]- b2['amount'][:-1].sum()
            list_js.append(b2)
    df_doi = pd.concat(list_js,ignore_index=True)

    df_doi['riqi'] = df_doi['riqi'].map(lambda x:str(x)[:10])
    df_doi['riqi'] = pd.to_datetime(df_doi['riqi'],format="%Y-%m-%d")

    df_doi['doi'] = date - df_doi['riqi']
    df_doi['doi'] = df_doi['doi'].map(lambda x:int(str(x).split(' ')[0]) if str(x) != 'NaT' else x)

    for i in range(len(df_doi)):
        if df_doi['inventory'][i]<=0:
            df_doi.loc[i,'doi'] = 0


    # dict_level = {
    #             '0-30天':[0,30],
    #             '30-90天':[30,90],
    #             '90-180天':[90,180],
    #             '180-360天':[180,360],
    #             '360天以上':[360,10000]}

    dict_level = {
                '0-180天':[-1,180],
                '180-360天':[180,360],
                '360天以上':[360,10000]}
    
    df_doi['level'] = df_doi['doi'].map(lambda x:getDict(dict_level,x,'无库存'))
    for i in range(len(df_doi)):
        if df_doi['surplus'][i] == 0:
            df_doi['level'][i] = '无库存'

    return df_doi

df_doi = funcA(df_warehouse,df_purchasereceiving,datetime.today())
df_warehouse['riqi'] = pd.to_datetime(df_warehouse['riqi'],format='%Y-%m-%d')
df_purchasereceiving['riqi'] = pd.to_datetime(df_purchasereceiving['riqi'],format='%Y-%m-%d')


a30 = df_warehouse[df_warehouse['riqi']<=datetime.today()-timedelta(30)]
b30 = df_purchasereceiving[df_purchasereceiving['riqi']<=datetime.today()-timedelta(30)]
df_doi_30 = funcA(a30,b30,datetime.today()-timedelta(30))


a60 = df_warehouse[df_warehouse['riqi']<=datetime.today()-timedelta(60)]
b60 = df_purchasereceiving[df_purchasereceiving['riqi']<=datetime.today()-timedelta(60)]
df_doi_60 = funcA(a60,b60,datetime.today()-timedelta(60))


a7 = df_warehouse[df_warehouse['riqi']<=datetime.today()-timedelta(7)]
b7 = df_purchasereceiving[df_purchasereceiving['riqi']<=datetime.today()-timedelta(7)]
df_doi_7 = funcA(a7,b7,datetime.today()-timedelta(7))



df_doi_7['refresh'] = datetime.now()
df_doi_30['refresh'] = datetime.now()
df_doi_60['refresh'] = datetime.now()


# *****************************************写入mysql*****************************************************#
# df_doi.to_sql('erp_jd_dws_doi',       engine, schema='erp_jd_dws', if_exists='replace',index=False)
# df_doi_30.to_sql('erp_jd_dws_doi_lastmonth',     engine, schema='erp_jd_dws', if_exists='replace',index=False)
# df_doi_60.to_sql('erp_jd_dws_doi_last2month',     engine, schema='erp_jd_dws', if_exists='replace',index=False)
# engine1.dispose()



savesql(df_doi_7,'erp_jd_dws','erp_jd_dws_doi_lastweek',"""CREATE TABLE `erp_jd_dws_doi_lastweek` (
  `wuliaomc` text,
  `riqi` datetime DEFAULT NULL,
  `amount` int DEFAULT NULL,
  `t_amount` int DEFAULT NULL,
  `inventory` double DEFAULT NULL,
  `surplus` double DEFAULT NULL,
  `doi` bigint DEFAULT NULL,
  `level` text,
  `refresh` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO erp_jd_dws_doi_lastweek(wuliaomc,riqi,amount,t_amount,inventory,surplus,doi,level,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)")



savesql(df_doi_30,'erp_jd_dws','erp_jd_dws_doi_lastmonth',"""CREATE TABLE `erp_jd_dws_doi_lastmonth` (
  `wuliaomc` text,
  `riqi` datetime DEFAULT NULL,
  `amount` int DEFAULT NULL,
  `t_amount` int DEFAULT NULL,
  `inventory` double DEFAULT NULL,
  `surplus` double DEFAULT NULL,
  `doi` bigint DEFAULT NULL,
  `level` text,
  `refresh` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO erp_jd_dws_doi_lastmonth(wuliaomc,riqi,amount,t_amount,inventory,surplus,doi,level,refresh)VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)")


savesql(df_doi_60,'erp_jd_dws','erp_jd_dws_doi_last2month',"""CREATE TABLE `erp_jd_dws_doi_last2month` (
  `wuliaomc` text,
  `riqi` datetime DEFAULT NULL,
  `amount` int DEFAULT NULL,
  `t_amount` int DEFAULT NULL,
  `inventory` double DEFAULT NULL,
  `surplus` double DEFAULT NULL,
  `doi` bigint DEFAULT NULL,
  `level` text,
  `refresh` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO erp_jd_dws_doi_last2month(wuliaomc,riqi,amount,t_amount,inventory,surplus,doi,level,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)")