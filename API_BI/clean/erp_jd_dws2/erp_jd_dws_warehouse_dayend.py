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
df_warehouse  = pd.read_sql_query(text('select * from erp_jd_dws.erp_jd_dws_warehouse;'), engine.connect())  
dlzy_inventory = pd.read_sql_query(text('select * from www_bi_ads.dlzy_inventory;'), engine.connect())  

b = pd.read_sql_query(text('select wlmc_all wuliaomc,riqi , sum(shifasl_new) amount from erp_jd_dwd.erp_jd_dwd_dim_purchasereceiving group by wlmc_all,riqi having sum(shifasl_new)<>0;'), engine.connect())
c = pd.read_sql_query(text("select wuliaomc,riqi,receiving amount from erp_jd_dwd.erp_jd_dwd_dim_beginninginventory;"), engine.connect())
# 其他入库路径的采购
df_d = pd.read_sql_query(text("""SELECT  a.wuliaomc,a.riqi,sum(a.shishousl) amount FROM `erp_jd_dwd`.`erp_jd_dwd_dim_othersreceiving` a
                                LEFT JOIN (
                                SELECT DISTINCT wlmc_all wuliaomc FROM `erp_jd_dwd`.`erp_jd_dwd_dim_purchasereceiving`
                                union
                                SELECT DISTINCT wuliaomc FROM `erp_jd_dwd`.`erp_jd_dwd_dim_purchasereceiving`
                                union
                                SELECT DISTINCT wuliaomc FROM `erp_jd_dwd`.`erp_jd_dwd_dim_beginninginventory`
                                ) b on a.wuliaomc = b.wuliaomc
                                where b.wuliaomc is null
                                GROUP BY a.wuliaomc,a.riqi;"""), engine.connect())


engine.dispose()

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








# df_doi   库存天数表
# ----------------------------------------------------------------------------------------------------- # 
def funcA(df_warehouse, date, b = b, c = c, df_d = df_d):  
    a = df_warehouse.groupby('wuliaomc',as_index=False)['inventory'].sum()
    d = pd.concat([c,b,df_d],ignore_index=True)

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

    df2['surplus'] = pd.Series([], dtype='float64')
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

    df_doi = df_doi[df_doi['surplus']!=0]

    return df_doi

df_doi = funcA(df_warehouse,datetime.today())




# df_doi_fc  分仓库存天数表
# ----------------------------------------------------------------------------------------------------- # 
med = df_warehouse.groupby(['wuliaomc'],as_index=False).agg({'inventory':'sum'})
df_warehouse = pd.merge(df_warehouse,med.rename(columns={'inventory':'med'}) ,on=['wuliaomc'],how='left')
df_warehouse = df_warehouse[df_warehouse['med']>0].drop(['med'],axis = 1)
med1 = df_warehouse.groupby(['wuliaomc','cangkumc'],as_index=False).agg({'inventory':'sum'})
df_warehouse = pd.merge(df_warehouse,med1.rename(columns={'inventory':'med'}) ,on=['wuliaomc','cangkumc'],how='left')
df_warehouse = df_warehouse[df_warehouse['med']>0].drop(['med'],axis = 1)


# 排序计算累计各仓库存
df_warehouse.sort_values(['wuliaomc','riqi','receiving'],ascending=[True,True,False],inplace=True)
b = df_warehouse.groupby(['wuliaomc','cangkumc','riqi'],as_index=False).agg({'receiving':'sum','shipping':'sum'})
b['inventory'] = b['receiving'] - b['shipping'] 
b['inventory_add'] = b.groupby(['wuliaomc','cangkumc'])['inventory'].cumsum()

# 去除库存为0数据，匹配当前库存列
c = b[b['inventory_add']>0]
c['riqi'] = pd.to_datetime(c['riqi'],format='%Y-%m-%d')
list_index = c.groupby(['wuliaomc','cangkumc'])['riqi'].idxmax().to_list()
d = c.loc[list_index][['wuliaomc','cangkumc','inventory_add']].rename(columns={'inventory_add':'inventory'})
c = pd.merge(c[['wuliaomc','cangkumc','riqi','receiving']],d,on=['wuliaomc','cangkumc'],how='left')
d = c[c['receiving']>0]
d.sort_values(['wuliaomc','cangkumc','riqi'],ascending=False,inplace=True)
d.loc[:,'receiving_add'] = d.groupby(['wuliaomc','cangkumc'])['receiving'].cumsum()

# 辅助列判断当前库存是否大于倒算累计入库
d.loc[:,'fz'] = d['receiving_add']-d['inventory']
d.loc[:,'fz'] = d['fz'].map(lambda x: 'y' if x >= 0 else 'n')
# 截小于入库的连接去重后的大于入库的
d_n = d[d['fz']=='n']
d_y = d[d['fz']=='y'].drop_duplicates(['wuliaomc','cangkumc'])
e = pd.concat([d_n,d_y]).sort_values(['wuliaomc','cangkumc','riqi'],ascending=False)

# 各仓库存天数
e['surplus'] = pd.Series([], dtype='float64')
list_e1 = []
for i in e['wuliaomc'].drop_duplicates():
    for j in e[e['wuliaomc']==i]['cangkumc'].drop_duplicates():
        e1 = e[(e['wuliaomc']==i)&(e['cangkumc']==j)].reset_index(drop = True)
        if len(e1)==1:
            e1['surplus'].fillna(e1['inventory'],inplace = True)
            list_e1.append(e1)
        else: 
            e1['surplus'].fillna(e1['receiving'],inplace = True)
            e1.loc[e1.index[-1],'surplus'] = e1['inventory'][-1:].values[0]- e1['receiving'][:-1].sum()
            list_e1.append(e1)
df_doi_fc = pd.concat(list_e1,ignore_index=True)

df_doi_fc['riqi'] = df_doi_fc['riqi'].map(lambda x:str(x)[:10])
df_doi_fc['riqi'] = pd.to_datetime(df_doi_fc['riqi'],format="%Y-%m-%d")

df_doi_fc['doi'] = datetime.today() - df_doi_fc['riqi']
df_doi_fc['doi'] = df_doi_fc['doi'].map(lambda x:int(str(x).split(' ')[0]) if str(x) != 'NaT' else x)

for i in range(len(df_doi_fc)):
    if df_doi_fc['inventory'][i]<=0:
        df_doi_fc.loc[i,'doi'] = 0


df_doi_fc['riqi'] = df_doi_fc['riqi'].map(lambda x:str(x)[:10])
df_doi_fc['riqi'] = pd.to_datetime(df_doi_fc['riqi'],format="%Y-%m-%d")

df_doi_fc['doi'] = datetime.today() - df_doi_fc['riqi']
df_doi_fc['doi'] = df_doi_fc['doi'].map(lambda x:int(str(x).split(' ')[0]) if str(x) != 'NaT' else x)

for i in range(len(df_doi_fc)):
    if df_doi_fc['inventory'][i]<=0:
        df_doi_fc.loc[i,'doi'] = 0

dict_level = {
            '0-30天':[-1,30],
            '30-90天':[30,90],
            '90-180天':[90,180],
            '180-360天':[180,360],
            '360天以上':[360,10000]}

df_doi_fc['level'] = df_doi_fc['doi'].map(lambda x:getDict(dict_level,x,'无库存'))





df_doi['refresh'] = datetime.now()
df_doi_fc['refresh'] = datetime.now()



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






savesql(df_doi,'erp_jd_dws','erp_jd_dws_doi',"""CREATE TABLE `erp_jd_dws_doi` (
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
"INSERT INTO erp_jd_dws_doi(wuliaomc,riqi,amount,t_amount,inventory,surplus,doi,level,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)")



savesql(df_doi_fc,'erp_jd_dws','erp_jd_dws_doi_fc',"""CREATE TABLE `erp_jd_dws_doi_fc` (
  `wuliaomc` text,
  `cangkumc` text,
  `riqi` datetime DEFAULT NULL,
  `receiving` double DEFAULT NULL,
  `inventory` double DEFAULT NULL,
  `receiving_add` double DEFAULT NULL,
  `fz` text,
  `surplus` double DEFAULT NULL,
  `doi` bigint DEFAULT NULL,
  `level` text,
  `refresh` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO erp_jd_dws_doi_fc(wuliaomc,cangkumc,riqi,receiving,inventory,receiving_add,fz,surplus,doi,level,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
