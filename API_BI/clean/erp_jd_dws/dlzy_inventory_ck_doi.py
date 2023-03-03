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
df_warehouse = pd.read_sql_query(text('SELECT * FROM www_bi_ads.dlzy_inventory;'), engine.connect())
a11          = pd.read_sql_query(text('select 客户名称 cangkumc,	物料名称 wuliaomc ,结余数量 inventory from  www_bi_dwd.jishou_wuliao_sale_stat;'), engine.connect())

engine.dispose()


df_warehouse['cangkumc'].replace({'重庆西西弗文化传播有限公司':'西西弗','贵州西西弗文化传播有限公司':'西西弗','杭州迷思文化创意有限公司-西西弗':'西西弗'},inplace=True)
a11['cangkumc'].replace({'重庆西西弗文化传播有限公司':'西西弗','贵州西西弗文化传播有限公司':'西西弗','杭州迷思文化创意有限公司-西西弗':'西西弗'},inplace=True)

a1 = a11.groupby(['cangkumc','wuliaomc'],as_index=False)['inventory'].sum()

b = df_warehouse[df_warehouse['类型']=='发货'].groupby(['cangkumc','wuliaomc','riqi'],as_index=False)['inventory'].sum()
d1 = b[b['inventory']!= 0].rename(columns={'inventory':'amount'})


d1['amount'] = d1['amount'].astype(int)
d1['riqi'] = d1['riqi'].map(lambda x:str(x)[:10])
d1['riqi'] = pd.to_datetime(d1['riqi'],format='%Y-%m-%d')




list_1 = []
for k in d1['cangkumc'].drop_duplicates():
    d = d1[d1['cangkumc']==k]
    a = a1[a1['cangkumc']==k]
    # 增加库存 
    mydict = dict(zip(a['wuliaomc'],a['inventory']))
    list_ = []
            
    for i in d['wuliaomc'].drop_duplicates():
        b1 = d[d['wuliaomc']==i]
        # print(b1)
        b1.sort_values(['riqi'],ascending=False,inplace=True,ignore_index=True)
        b1.reset_index(drop = True,inplace = True)
        # 累计采购及期初数量
        b1.loc[:,'t_amount'] = b1['amount'].cumsum()

        for j in range(len(b1)):
            if getDictKey1(mydict,b1['wuliaomc'][j],0)<=b1['t_amount'][j]:
                list_.append(b1.loc[:j])
                break
                
    df1 = pd.concat(list_,ignore_index=True)

    df2 = pd.merge(df1,a,on=['cangkumc','wuliaomc'],how='left')
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

    df_doi['doi'] = datetime.now() - df_doi['riqi']
    df_doi['doi'] = df_doi['doi'].map(lambda x:int(str(x).split(' ')[0]) if str(x) != 'NaT' else x)

    for i in range(len(df_doi)):
        if df_doi['inventory'][i]<=0:
            df_doi.loc[i,'doi'] = 0


    dict_level = {
                '0-180天':[-1,180],
                '180-360天':[180,360],
                '360天以上':[360,10000]}
    # print(df_doi)
    df_doi['level'] = df_doi['doi'].map(lambda x:getDict(dict_level,x,'无库存'))
    for i in range(len(df_doi)):
        if df_doi['surplus'][i] == 0:
            df_doi['level'][i] = '无库存'


    list_1.append(df_doi)
df_doi = pd.concat(list_1,ignore_index=True)


df_doi.to_sql('dlzy_inventory_ck_doi', engine, schema='www_bi_ads', if_exists='replace',index=False) 