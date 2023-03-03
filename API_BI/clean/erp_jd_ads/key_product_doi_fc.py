
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine,text



# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306')) 


# *****************************************取数据********************************************************#
doi         = pd.read_sql_query(text('select * from erp_jd_dws.erp_jd_dws_doi;'), engine.connect())
key_cangku  = pd.read_sql_query(text('SELECT * FROM `erp_jd_ads`.`key_cangku` where 库存数量 > 0 ;'), engine.connect())
df_classify = pd.read_sql_query(text('SELECT * FROM `erp_jd_dwd`.`erp_jd_dwd_fact_classify` ;'), engine.connect())
assemble    = pd.read_sql_query(text("""
                                    SELECT DISTINCT wuliaomc,shiwulx,fid,danjubh FROM `erp_jd_dwd`.`erp_jd_dwd_dim_assemble`
                                    where danjubh in (
                                    SELECT DISTINCT a.danjubh FROM `erp_jd_dwd`.`erp_jd_dwd_dim_assemble` a
                                    LEFT JOIN (
                                    SELECT DISTINCT wlmc_all wuliaomc FROM `erp_jd_dwd`.`erp_jd_dwd_dim_purchasereceiving`
                                    union
                                    SELECT DISTINCT wuliaomc FROM `erp_jd_dwd`.`erp_jd_dwd_dim_purchasereceiving`
                                    union
                                    SELECT DISTINCT wuliaomc FROM `erp_jd_dwd`.`erp_jd_dwd_dim_othersreceiving`
                                    union
                                    SELECT DISTINCT wuliaomc FROM `erp_jd_dwd`.`erp_jd_dwd_dim_beginninginventory`
                                    ) b on a.wuliaomc = b.wuliaomc
                                    where b.wuliaomc is null
                                    AND a.shiwulx = '组装'
                                    and a.wuliaomc not like '%%装配'
                                    ) 
                                    and wuliaomc not in ('防伪贴','三国杀自封袋','塑封珍藏版内衬','塑封外盒热缩袋')
                                    order by fid,danjubh;"""), engine.connect())


# 区分剩余单批次和多批次入库物料
doi_count = doi.groupby(['wuliaomc'])['wuliaomc'].count()
a1 = doi[doi['wuliaomc'].isin(doi_count[doi_count.values==1].index)]
a2 = doi[doi['wuliaomc'].isin(doi_count[doi_count.values>1].index)]


# 只剩最后一批次采购入库物料的doi匹配物料收发明细计算
# 单批次库龄表
doi_01 = pd.merge(a1[['doi','wuliaomc']].rename(columns={'wuliaomc':'物料名称'}),key_cangku[['物料名称','仓库','库存数量']],on=['物料名称'],how='left')

# 剩余多批次采购入库物料的doi匹配物料收发明细计算
# 多批次库龄表
doi_02 = pd.merge(a2[['wuliaomc','surplus','doi']].drop_duplicates().rename(columns={'wuliaomc':'物料名称','surplus':'库存数量'}),key_cangku[['物料名称','仓库','库存数量']],on=['物料名称','库存数量'],how='left')
# 1.
doi_021 = doi_02[~doi_02['仓库'].isna()]
b1 = doi_02[doi_02['仓库'].isna()]
b2 = pd.merge(b1.groupby(['物料名称'],as_index=False).agg({'库存数量':'sum'}),key_cangku[['物料名称','仓库','库存数量']],on=['物料名称','库存数量'],how='left')
# 2.
doi_022 = pd.merge(b2[~b2['仓库'].isna()][['物料名称','仓库']],b1[['物料名称','库存数量','doi']],on=['物料名称'],how='left')
c1 = b1[b1['物料名称'].isin(b2[b2['仓库'].isna()]['物料名称'].to_list())]

# 多批次仓库库存表
cangku_01 = pd.merge(key_cangku[~key_cangku['物料名称'].isin(doi_01 ['物料名称'].to_list())][['物料名称','仓库','库存数量']],a2[['wuliaomc','surplus','doi']].drop_duplicates().rename(columns={'wuliaomc':'物料名称','surplus':'库存数量'}),on=['物料名称','库存数量'],how='left')
d1 = cangku_01[cangku_01['doi'].isna()]

doi_022['仓库1']=doi_022['仓库']
cangku_02 = pd.merge(d1[['物料名称','仓库','库存数量']],doi_022[['物料名称','仓库','仓库1']].drop_duplicates(),on=['物料名称','仓库'],how='left')
# 
d2 = cangku_02[cangku_02['仓库1'].isna()]
d2 = pd.merge(d2,d2.set_index(['物料名称','仓库'])['库存数量'].div(d2.groupby(['物料名称'])['库存数量'].sum(), axis=0).reset_index().rename(columns={'库存数量':'库存比例'}),on = ['物料名称','仓库'],how='left')
d3 = pd.merge(d2[['物料名称','仓库','库存数量','库存比例']],c1 [['物料名称','doi','库存数量']].rename(columns={'库存数量':'库龄数量'}),on=['物料名称'],how='left')
d3['仓库库龄对应库存'] = round(d3['库龄数量']*d3['库存比例'],0)

doi_023 = d3[~d3['doi'].isna()]
d4 = d3[d3['doi'].isna()]

# 增加库存参考物料
assemble1 = pd.merge(assemble[assemble['shiwulx']=='组装'].rename(columns={'wuliaomc':'成品名称'}),assemble[assemble['shiwulx']=='组装子件'],on=['fid','danjubh'],how='left')
assemble1 = assemble1[assemble1['wuliaomc'].isin(key_cangku['物料名称'].to_list())]
d5 = pd.merge(d4,assemble1[['成品名称','wuliaomc']].drop_duplicates().rename(columns={'成品名称':'物料名称'}),on=['物料名称'],how='left')
doi_024 = d5[~d5['wuliaomc'].isna()]


# 合表
ck_doi = pd.concat([doi_01,doi_021,doi_022.drop(['仓库1'],axis = 1),doi_023[['物料名称','仓库','doi','仓库库龄对应库存']].rename(columns = {'仓库库龄对应库存':'库存数量'}),doi_024[['物料名称','仓库','库存数量','doi','wuliaomc']].rename(columns = {'wuliaomc':'库龄参考物料'})])
ck_doi.rename(columns = {'doi':'库龄天数'},inplace=True)


ck_doi.to_sql('key_product_doi_fc',          engine, schema='erp_jd_ads', if_exists='replace',index=False) 

engine.dispose()