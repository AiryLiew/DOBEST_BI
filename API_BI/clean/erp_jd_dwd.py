# -*- coding: utf-8 -*-
# 测试环境: python3.9.6



# *****************************************自定义函数路径*************************************************#
import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

from key_tab import savesql,clean,merge_label,getDictKey,area,cf,ct
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine,text


print("\n","START DWD", datetime.now(),"\n")


# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306')) 


# *****************************************取数据********************************************************#
finish_order                = pd.read_sql_query(text('select * from localdata.finish_order;'),                             engine.connect())
customer_name_change        = pd.read_sql_query(text('select * from localdata.customer_name_change;'),                     engine.connect())
df_wuliaofzid               = pd.read_sql_query(text('select * from erp_jd_dwd.erp_jd_dwd_fact_wuliaofzid;'),              engine.connect())
df_voucherpayable           = pd.read_sql_query(text('select * from erp_jd_dwd.erp_jd_dwd_dim_voucherpayable;'),           engine.connect())
df_cost                     = pd.read_sql_query(text('SELECT * FROM `erp_jd_dwd`.`erp_jd_dwd_dim_cost`;'),                 engine.connect()) 
df_wlys                     = pd.read_sql_query(text('SELECT * FROM `erp_jd_dwd`.`erp_jd_dwd_fact_wuliaomc_ys`;'),         engine.connect()) 

df_purchaseOrders_wc_dobest = pd.read_sql_query(text('select * from erp_jd_ods.erp_jd_ods_dim_purchaseorders_wc_dobest;'), engine.connect())  
df_purchaseOrders_wc_cwzx   = pd.read_sql_query(text('select * from erp_jd_ods.erp_jd_ods_dim_purchaseorders_wc_cwzx;'),   engine.connect()) 



df_client = pd.read_sql_query(text("""select * from erp_jd_ods.erp_jd_ods_fact_client_wc_dobest where shenhezt = '已审核'
                                union all 
                                select * from erp_jd_ods.erp_jd_ods_fact_client_wc_cwzx where shenhezt = '已审核'
                                union all 
                                select * from erp_jd_ods.erp_jd_ods_fact_client_ms_dobest where shenhezt = '已审核'
                                union all
                                select * from erp_jd_ods.erp_jd_ods_fact_client_ms_cwzx where shenhezt = '已审核'
                                union all 
                                select * from erp_jd_ods.erp_jd_ods_fact_client_yc_xmgs where shenhezt = '已审核'
                                union all 
                                select * from erp_jd_ods.erp_jd_ods_fact_client_yc_cwzx where shenhezt = '已审核'
                                union all 
                                select * from erp_jd_ods.erp_jd_ods_fact_client_kyk_cwzx where shenhezt = '已审核';"""), engine.connect()) 


df_classify = pd.read_sql_query(text("""select * from erp_jd_ods.erp_jd_ods_fact_classify_wc_dobest where shenhezt = '已审核'
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_fact_classify_wc_cwzx where shenhezt = '已审核'
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_fact_classify_ms_dobest where shenhezt = '已审核'
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_fact_classify_ms_cwzx where shenhezt = '已审核'
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_fact_classify_yc_xmgs where shenhezt = '已审核'
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_fact_classify_yc_cwzx where shenhezt = '已审核'
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_fact_classify_kyk_cwzx where shenhezt = '已审核';"""), engine.connect()) 


df_saleOrders = pd.read_sql_query(text("""select * from erp_jd_ods.erp_jd_ods_dim_saleorders_wc_dobest
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_dim_saleorders_wc_cwzx
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_dim_saleorders_ms_dobest
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_dim_saleorders_ms_cwzx
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_dim_saleorders_yc_xmgs
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_dim_saleorders_yc_cwzx
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_dim_saleorders_kyk_cwzx;"""), engine.connect())  


df_saleReturn = pd.read_sql_query(text("""select * from erp_jd_ods.erp_jd_ods_dim_salereturn_wc_dobest where wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_dim_salereturn_wc_cwzx where wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_dim_salereturn_yc_xmgs where wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_dim_salereturn_yc_cwzx where wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_dim_salereturn_ms_dobest where wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_dim_salereturn_kyk_cwzx where wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中');"""),   engine.connect()) 


df_consignment = pd.read_sql_query(text("""select * from erp_jd_ods.erp_jd_ods_dim_consignment_wc_dobest where danjubh <> " "
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_consignment_wc_cwzx where danjubh <> " "
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_consignment_ms_cwzx where danjubh <> " ";"""),   engine.connect()) 


df_saleShipping = pd.read_sql_query(text("""select * from erp_jd_ods.erp_jd_ods_dim_saleshipping_wc_dobest where shenhezt <> '其他' and wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_saleshipping_wc_cwzx1 where wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_saleshipping_wc_cwzx where wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_saleshipping_ms_dobest where wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_saleshipping_ms_cwzx where wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_saleshipping_yc_xmgs where wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_saleshipping_yc_cwzx where wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_saleshipping_kyk_cwzx where wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中');"""),   engine.connect()) 


df_purchaseReturn = pd.read_sql_query(text("""select * from erp_jd_ods.erp_jd_ods_dim_purchasereturn_wc_dobest
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_purchasereturn_wc_cwzx
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_purchasereturn_ms_cwzx
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_purchasereturn_yc_xmgs
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_purchasereturn_yc_cwzx
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_purchasereturn_kyk_cwzx;"""), engine.connect()) 


df_purchaseOrders = pd.read_sql_query(text("""select * from erp_jd_ods.erp_jd_ods_dim_purchaseorders_wc_dobest where gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司') and danjubh not in ('CGDD002945','CGDD002946','CGDD002947','CGDD002948','CGDD002949','CGDD002950','CGDD002951','CGDD002952','CGDD002953','CGDD0005762') and shenhezt = '已审核'
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_purchaseorders_wc_cwzx where gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司') and danjubh not in ('CGDD002945','CGDD002946','CGDD002947','CGDD002948','CGDD002949','CGDD002950','CGDD002951','CGDD002952','CGDD002953','CGDD0005762') and shenhezt = '已审核'
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_purchaseorders_ms_dobest where gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司') and danjubh not in ('CGDD002945','CGDD002946','CGDD002947','CGDD002948','CGDD002949','CGDD002950','CGDD002951','CGDD002952','CGDD002953','CGDD0005762') and shenhezt = '已审核'
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_purchaseorders_ms_cwzx where gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司') and danjubh not in ('CGDD002945','CGDD002946','CGDD002947','CGDD002948','CGDD002949','CGDD002950','CGDD002951','CGDD002952','CGDD002953','CGDD0005762') and shenhezt = '已审核'
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_purchaseorders_yc_xmgs where gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司') and danjubh not in ('CGDD002945','CGDD002946','CGDD002947','CGDD002948','CGDD002949','CGDD002950','CGDD002951','CGDD002952','CGDD002953','CGDD0005762') and shenhezt = '已审核'
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_purchaseorders_yc_cwzx where gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司') and danjubh not in ('CGDD002945','CGDD002946','CGDD002947','CGDD002948','CGDD002949','CGDD002950','CGDD002951','CGDD002952','CGDD002953','CGDD0005762') and shenhezt = '已审核'
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_purchaseorders_kyk_cwzx where gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司') and danjubh not in ('CGDD002945','CGDD002946','CGDD002947','CGDD002948','CGDD002949','CGDD002950','CGDD002951','CGDD002952','CGDD002953','CGDD0005762') and shenhezt = '已审核';"""),   engine.connect()) 


df_purchaseReceiving = pd.read_sql_query(text("""select * from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_wc_dobest where gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司')
                                            union all 
                                            select * from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_wc_dobest where danjubh = 'CGRK07101' and dingdandh = ' '
                                            union all
                                            select * from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_wc_cwzx where gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司')
                                            union all 
                                            select * from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_ms_dobest where gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司')
                                            union all 
                                            select * from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_ms_cwzx where gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司')
                                            union all 
                                            select * from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_yc_xmgs where gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司')
                                            union all 
                                            select * from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_yc_cwzx where gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司')
                                            union all 
                                            select * from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_kyk_cwzx where gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司');"""),   engine.connect()) 



engine.dispose() 


df_voucherpayable['riqi'] = pd.to_datetime(df_voucherpayable['riqi'],format='%Y-%m-%d')


# ******************************************清洗表*******************************************************#


# df_classify   物料分类表（区分欢乐坊、三国杀、yokakids、其他）
# ----------------------------------------------------------------------------------------------------- # 
df_classify.dropna(subset = ['wuliaofzid'],axis=0,inplace=True)
df_classify['wuliaofzid_2'] = df_classify['wuliaofzid'].map(lambda x :x[:6] if len(x)>=6 else x)
df_classify = pd.merge(df_classify ,df_wuliaofzid[['wuliaofzid_1', 'wuliaofzmc_1','wuliaofzid_2', 'wuliaofzmc_2']],on=['wuliaofzid_2'],how='left')
df_classify = pd.merge(df_classify ,df_wuliaofzid[['wuliaofzid_3', 'wuliaofzmc_3']],left_on=['wuliaofzid'],right_on=['wuliaofzid_3'],how='left')
df_classify.rename(columns={'wuliaofzmc_1':'classify', 'wuliaofzmc_2':'classify_1','wuliaofzmc_3':'classify_2'},inplace=True)
df_classify.sort_values(['wuliaomc','classify'],inplace=True)
df_classify = df_classify[df_classify['wuliaomc'].duplicated()==False]
df_classify.sort_values(['wuliaofzid'],ascending=False ,inplace=True)
df_classify = df_classify[df_classify['wuliaobm'].duplicated()==False]
df_classify.drop(['refresh_jk','fid'],axis=1,inplace = True)
df_classify['classify_2'].fillna(df_classify['classify_1'],inplace=True)

# 核心桌游停用
df_classify = df_classify[df_classify['wuliaofzid']!='0107']

print('df_classify:',datetime.now())



# df_saleShipping  销售出库表
# ----------------------------------------------------------------------------------------------------- #
# 销售清洗，增加部门、成本、新品分类
df_saleShipping = clean(df_saleShipping,df_cost,'bumenmc','shifasl')

# 客户原名称统一更改为新名称
df_saleShipping = ct(df_saleShipping)
# 修正物料名称
df_saleShipping = cf(df_saleShipping)

df_saleShipping.drop(['refresh_jk','fid'],axis=1,inplace = True)

print('df_saleShipping:',datetime.now())


# df_purchaseOrders      采购订单表（新物料名称wlmc_all匹配为销售物料名称）
# ----------------------------------------------------------------------------------------------------- #
# 修正物料名称
df_purchaseOrders = cf(df_purchaseOrders)

# 由账套更换引起的重复单据去除
# 核心桌游采购单价变动，以财务聂挺的金额为准
list_dj = df_purchaseOrders[(df_purchaseOrders['chuangjianrmc']=='聂挺')&(df_purchaseOrders['wuliaofzid']=='030801')]['danjubh'].to_list()
list_bm = df_purchaseOrders[(df_purchaseOrders['chuangjianrmc']=='聂挺')&(df_purchaseOrders['wuliaofzid']=='030801')]['wuliaobm'].to_list()
for i in range(len(df_purchaseOrders)):
    if df_purchaseOrders['chuangjianrmc'][i]!='聂挺' and df_purchaseOrders['danjubh'][i] in list_dj and df_purchaseOrders['wuliaobm'][i] in list_bm:
        df_purchaseOrders.loc[i,'caigousl'] = df_purchaseOrders['leijirksl'][i]
        df_purchaseOrders.loc[i,'jiashuihj'] = df_purchaseOrders['leijirksl'][i]*df_purchaseOrders['hanshuidj'][i]
    elif df_purchaseOrders['chuangjianrmc'][i] in ['聂挺','张则璐']  and df_purchaseOrders[df_purchaseOrders['danjubh'] == df_purchaseOrders['danjubh'][i]]['danjubh'].count()>1 and df_purchaseOrders[df_purchaseOrders['wuliaobm'] == df_purchaseOrders['wuliaobm'][i]]['wuliaobm'].count()>1 and df_purchaseOrders[df_purchaseOrders['wuliaomc'] == df_purchaseOrders['wuliaomc'][i]]['wuliaomc'].count()>1 and df_purchaseOrders['danjubh'][i] not in list_dj and df_purchaseOrders['wuliaobm'][i] not in list_bm:
        df_purchaseOrders.loc[i,'caigousl'] = 0
        df_purchaseOrders.loc[i,'jiashuihj'] = 0
        

# 自定义函数，增加可与销售匹配的物料名称                                                                         
df_purchaseOrders = merge_label(df_purchaseOrders, df_saleShipping['wuliaomc'].drop_duplicates().to_list(),'caigousl','riqi',df_wlys)
# 新增未入库列，真实采购单价，处理掉重复值
df_purchaseOrders.reset_index(drop = True,inplace = True)

# 判断成品，计算其采购量
list_cnew = []
list_lnew = []
list_hsdj = []
for i in range(len(df_purchaseOrders)):
    if df_purchaseOrders['wuliaobm'][i][:2] in ['01','03']:
        list_cnew.append(df_purchaseOrders['shengyurksl'][i])
        list_lnew.append(df_purchaseOrders['leijirksl'][i])
        list_hsdj.append(df_purchaseOrders['hanshuidj'][i])
    elif df_purchaseOrders['label'][i] == '装配':
        list_cnew.append(df_purchaseOrders['shengyurksl'][i])
        list_lnew.append(df_purchaseOrders['leijirksl'][i])
        list_hsdj.append(0)
    else:
        list_cnew.append(0)
        list_lnew.append(0)
        list_hsdj.append(0)
df_purchaseOrders['shengyurksl_new'] = pd.DataFrame(list_cnew)   
df_purchaseOrders['leijirksl_new'] = pd.DataFrame(list_lnew)   
df_purchaseOrders['hanshuidj_new'] = pd.DataFrame(list_hsdj) 
# 由账套更换引起的剩余入库数量数据修改
df_purchaseOrders['danjubh_1'] = df_purchaseOrders['danjubh'].map(lambda x:x if str(x)[-2:]!='-1' else str(x)[:-2])

for i in range(len(df_purchaseOrders)):
    if df_purchaseOrders['chuangjianrmc'][i] not in ['聂挺','张则璐']  and df_purchaseOrders[(df_purchaseOrders['danjubh_1'] == df_purchaseOrders['danjubh_1'][i])&(df_purchaseOrders['wuliaomc'] == df_purchaseOrders['wuliaomc'][i])]['danjubh_1'].count()>1 and df_purchaseOrders['riqi'][i]<datetime(2023,1,1):
        df_purchaseOrders.loc[i,'shengyurksl_new'] = 0
    elif df_purchaseOrders['gongyingsmc'][i] in ['杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司','上海飞之火电竞信息科技有限公司']:
        df_purchaseOrders.loc[i,'shengyurksl_new'] = 0


# 增加真实采购单价  
df_purchaseOrders01 = df_purchaseOrders[df_purchaseOrders['hanshuidj_new'] != 0]
df_purchaseOrders02 = df_purchaseOrders[df_purchaseOrders['hanshuidj_new'] == 0]
df_purchaseOrders02 = df_purchaseOrders02.drop(['hanshuidj_new'],axis=1)
# 以日期、产品、含税单价删除重复值
df_purchaseOrders_drop = df_purchaseOrders02[['wlmc_all','riqi','wuliaomc','hanshuidj']].drop_duplicates()
# 以日为单位计算真实采购单价                                                                                   
df_purchaseOrders_p = df_purchaseOrders_drop.groupby(['wlmc_all','riqi'], as_index = False)['hanshuidj'].sum()                                                                                   
df_purchaseOrders02 = pd.merge(df_purchaseOrders02, df_purchaseOrders_p, on=['wlmc_all','riqi'], how = 'left')
df_purchaseOrders02.rename(columns={'hanshuidj_x':'hanshuidj','hanshuidj_y':'hanshuidj_new'},inplace=True) 
df_purchaseOrders = pd.concat([df_purchaseOrders01,df_purchaseOrders02],ignore_index=True)
# 特定数据清理
df_purchaseOrders.reset_index(drop=True,inplace=True)
df_purchaseOrders['riqi'] = pd.to_datetime(df_purchaseOrders['riqi'], format='%Y-%m-%d')
for i in range(len(df_purchaseOrders)):
    if df_purchaseOrders['riqi'][i] >= datetime(2019,5,1) and df_purchaseOrders['wuliaomc'][i] == '塔罗牌局部闪磨砂UV':
        df_purchaseOrders.loc[i,'wlmc_all'] = 'LM-塔罗牌经典版'
    elif df_purchaseOrders['wlmc_all'][i] == 'H6-德国心脏病':
        df_purchaseOrders.loc[i,'caigousl_new'] = 5000
        df_purchaseOrders.loc[i,'hanshuidj_new'] = 1.93
    elif df_purchaseOrders['wlmc_all'][i] == '欢乐坊之终极狼人':
        df_purchaseOrders.loc[i,'caigousl_new'] = 4700
        df_purchaseOrders.loc[i,'hanshuidj_new'] = 2.47



# 空物料名称增补
for i in range(len(df_purchaseOrders)):
    if df_purchaseOrders['wlmc_all'][i] is np.nan and df_purchaseOrders['wuliaomc'][i][-3:] not in ['-卡牌','-外盒'] :
        df_purchaseOrders.loc[i,'wlmc_all']= df_purchaseOrders['wuliaomc'][i]
        df_purchaseOrders.loc[i,'shengyurksl_new'] = df_purchaseOrders['shengyurksl'][i]
        df_purchaseOrders.loc[i,'leijirksl_new'] = df_purchaseOrders['leijirksl'][i]
        df_purchaseOrders.loc[i,'hanshuidj_new'] = df_purchaseOrders['hanshuidj'][i]
        df_purchaseOrders.loc[i,'caigousl_new'] = df_purchaseOrders['caigousl'][i]
    elif df_purchaseOrders['wlmc_all'][i] is np.nan and df_purchaseOrders['wuliaomc'][i][-3:]  in ['-卡牌','-外盒']:
        df_purchaseOrders.loc[i,'wlmc_all']= df_purchaseOrders['wuliaomc'][i][:-3]
        df_purchaseOrders.loc[i,'shengyurksl_new'] = df_purchaseOrders['shengyurksl'][i]
        df_purchaseOrders.loc[i,'leijirksl_new'] = df_purchaseOrders['leijirksl'][i]
        df_purchaseOrders.loc[i,'hanshuidj_new'] = df_purchaseOrders['hanshuidj'][i]
        df_purchaseOrders.loc[i,'caigousl_new'] = df_purchaseOrders['caigousl'][i]

for i in range(len(df_purchaseOrders)):
    if df_purchaseOrders['wlmc_all'][i][-3:] =='-卡牌':
        df_purchaseOrders.loc[i,'wlmc_all']= df_purchaseOrders['wlmc_all'][i][:-3]



# 根据是否有对应应付单判断单据是否关闭
for i in range(len(df_purchaseOrders)):
    if df_purchaseOrders['guanbizt'][i] == '已关闭' or df_purchaseOrders['danjubh_1'][i] in finish_order['danjubh'].to_list() or (df_purchaseOrders['riqi'][i]<datetime(2021,5,1) and df_purchaseOrders['danjubh_1'][i] in list(set(df_voucherpayable['caigouddh_1']))):
        df_purchaseOrders.loc[i,'shengyurksl_new'] = 0


# 账套更换后名字变更的统一
data_hxzy_cwzx = df_purchaseOrders_wc_cwzx[['wuliaobm','wuliaomc']].drop_duplicates()
data_hxzy_dobest = df_purchaseOrders_wc_dobest[df_purchaseOrders_wc_dobest['wuliaobm'].isin(data_hxzy_cwzx['wuliaobm'])][['wuliaobm','wuliaomc']].drop_duplicates()
data_hxzy = pd.merge(data_hxzy_cwzx,data_hxzy_dobest,on=['wuliaobm'],how='right')
dict_mc = dict(zip(data_hxzy['wuliaomc_y'],data_hxzy['wuliaomc_x']))

df_purchaseOrders['wlmc_all'].replace(dict_mc,inplace=True)
df_purchaseOrders['wlmc_all'] = df_purchaseOrders['wlmc_all'].map(lambda x:x.rstrip())
df_purchaseOrders.drop(['refresh_jk','fid','wlmc_new','yewugb'],axis=1,inplace = True)

# 修正物料名称
df_purchaseOrders = cf(df_purchaseOrders)

print('df_purchaseOrders:',datetime.now())


# 年平均采购成本计算
df_purchaseOrders_cost = df_purchaseOrders.groupby(['year','wlmc_all'],as_index=False).agg({'jiashuihj':'sum','caigousl_new':'sum'})
df_purchaseOrders_cost = df_purchaseOrders_cost[df_purchaseOrders_cost['caigousl_new']!= 0]
df_purchaseOrders_cost['cost'] = round(df_purchaseOrders_cost['jiashuihj']/df_purchaseOrders_cost['caigousl_new'],2)

df_purchasecost = df_purchaseOrders_cost[['wlmc_all','year','cost']]
df_purchasecost = df_purchasecost.pivot(index='wlmc_all', columns='year', values='cost')
df_purchasecost.fillna(method='ffill', axis=1,inplace=True)
df_purchasecost.fillna(method='bfill', axis=1,inplace=True)
df_purchasecost = pd.DataFrame(df_purchasecost.stack())
df_purchasecost.reset_index(inplace=True)
df_purchasecost.rename(columns={0:'cost'},inplace=True)


# df_purchaseReceiving   采购入库表
# ----------------------------------------------------------------------------------------------------- #
# 修正物料名称
df_purchaseReceiving = cf(df_purchaseReceiving)

df_purchaseReceiving['shifasl'] = df_purchaseReceiving['shifasl'].astype(float)
df_purchaseReceiving = merge_label(df_purchaseReceiving,df_saleShipping['wuliaomc'].drop_duplicates().to_list(), 'shifasl','riqi',df_wlys)
# 空物料名称增补
df_purchaseReceiving['wlmc_all'].fillna(df_purchaseReceiving['wuliaomc'],inplace=True)

df_purchaseReceiving.reset_index(drop=True,inplace=True)
df_purchaseReceiving.drop(['refresh_jk','fid','wlmc_new'],axis=1,inplace = True)


print('df_purchaseReceiving:',datetime.now())


# df_purchaseReturn   采购退料表
# ----------------------------------------------------------------------------------------------------- #
# 修正物料名称
df_purchaseReturn = cf(df_purchaseReturn)

df_purchaseReturn = merge_label(df_purchaseReturn,df_saleShipping['wuliaomc'].drop_duplicates().to_list(), 'shituisl','tuiliaorq',df_wlys)
df_purchaseReturn.drop(['refresh_jk','fid','wlmc_new'],axis=1,inplace = True)


print('df_purchaseReturn:',datetime.now())


# df_client   区域表
# ----------------------------------------------------------------------------------------------------- # 
dict_client = {
    '零售事业组':['便利','百货','超市','连锁','书店','母婴系统','教育系统','新零售'],  
    '批发流通事业组':['东北','华北','华东','华南','华中','西南','西北'] 
    }
df_client['businessarea'] = df_client['kehufzmc'].map(lambda x :getDictKey(dict_client,x,'其他'))
df_client['kehumc'] = df_client['kehumc'].map(lambda x:x.rsplit()[0])

# 客户地区表
df_customerarea = df_saleOrders[['kehumc','shouhuofdz']].drop_duplicates()
df_client = pd.merge(df_client,df_customerarea,on=['kehumc'],how='left')
# 增加省市
df_client = area(df_client)
df_client.sort_values(['kehumc','chuangjiansj','kehufzmc'],ascending=False,inplace=True)
df_client1 = df_client[df_client.duplicated('kehumc')==False]
df_client1 = df_client1.drop(['name_coun1','refresh_jk','fid'],axis = 1)
df_client = df_client.drop(['name_coun1','refresh_jk','fid'],axis = 1)

print('df_client:',datetime.now())


# df_saleReturn  销售退货表
# ----------------------------------------------------------------------------------------------------- #
# 销售清洗，增加部门、成本、新品分类
df_saleReturn = clean(df_saleReturn,df_cost,'bumenmc','shifasl')

# 客户原名称统一更改为新名称
df_saleReturn = ct(df_saleReturn)
df_saleReturn.drop(['refresh_jk','fid'],axis=1,inplace = True)

# 修正物料名称
df_saleReturn = cf(df_saleReturn)

print('df_saleReturn:',datetime.now())


# df_saleOrders  销售订单表
# ----------------------------------------------------------------------------------------------------- #
df_saleOrders['return_am'] = df_saleOrders['hanshuidj']*df_saleOrders['leijithslxs']
df_saleOrders['jiashuihj_ac'] = df_saleOrders['jiashuihj'] - df_saleOrders['return_am']
df_saleOrders['xiaoshousl_ac'] = df_saleOrders['xiaoshousl'] - df_saleOrders['leijithslxs']

df_saleOrders = clean(df_saleOrders,df_cost,'xiaoshoubmmc','xiaoshousl')

df_saleOrders['purchases_ac'] = df_saleOrders['cost']*df_saleOrders['xiaoshousl_ac']
df_saleOrders['profit_ac'] = df_saleOrders['jiashuihj_ac']-df_saleOrders['purchases_ac']
df_saleOrders = df_saleOrders[~df_saleOrders['wuliaomc'].isin(['代收运费','测试物料1','管易云运费'])]

# 客户原名称统一更改为新名称
df_saleOrders = ct(df_saleOrders)
df_saleOrders.drop(['refresh_jk','fid'],axis=1,inplace = True)

# 修正物料名称
df_saleOrders = cf(df_saleOrders)

print('df_saleOrders:',datetime.now())


# df_consignment  寄售订单表
# ----------------------------------------------------------------------------------------------------- #
df_consignment = clean(df_consignment,df_cost,'xiaoshoubmmc','hangbencijssl')

# 客户原名称统一更改为新名称
df_consignment = ct(df_consignment)
df_consignment.drop(['refresh_jk','fid'],axis=1,inplace = True)

# 修正物料名称
df_consignment = cf(df_consignment)

print('df_consignment:',datetime.now())



# *****************************************数据更新增加*****************************************************# 
df_client['refresh'] = datetime.now()
df_client1['refresh'] = datetime.now()
df_classify['refresh'] = datetime.now()
df_saleOrders['refresh'] = datetime.now()
df_saleReturn['refresh'] = datetime.now()
df_consignment['refresh'] = datetime.now()
df_purchasecost['refresh'] = datetime.now()
df_saleShipping['refresh'] = datetime.now()     
df_purchaseOrders['refresh'] = datetime.now()  
df_purchaseReturn['refresh'] = datetime.now()
df_purchaseReceiving['refresh'] = datetime.now()


# *****************************************写入mysql*****************************************************# 
# df_client           .to_sql('erp_jd_dwd_fact_client',          engine1, schema='erp_jd_dwd', if_exists='replace',index=False) 
# df_classify         .to_sql('erp_jd_dwd_fact_classify',        engine1, schema='erp_jd_dwd', if_exists='replace',index=False) 
# df_saleOrders       .to_sql('erp_jd_dwd_dim_saleorders',       engine1, schema='erp_jd_dwd', if_exists='replace',index=False) 
# df_saleReturn       .to_sql('erp_jd_dwd_dim_salereturn',       engine1, schema='erp_jd_dwd', if_exists='replace',index=False)
# df_consignment      .to_sql('erp_jd_dwd_dim_consignment',      engine1, schema='erp_jd_dwd', if_exists='replace',index=False)
# df_purchasecost     .to_sql('erp_jd_dwd_dim_purchasecost',     engine1, schema='erp_jd_dwd', if_exists='replace',index=False)
# df_saleShipping     .to_sql('erp_jd_dwd_dim_saleshipping',     engine1, schema='erp_jd_dwd', if_exists='replace',index=False)     
# df_purchaseOrders   .to_sql('erp_jd_dwd_dim_purchaseorders',   engine1, schema='erp_jd_dwd', if_exists='replace',index=False)  
# df_purchaseReturn   .to_sql('erp_jd_dwd_dim_purchasereturn',   engine1, schema='erp_jd_dwd', if_exists='replace',index=False) 
# df_purchaseReceiving.to_sql('erp_jd_dwd_dim_purchasereceiving',engine1, schema='erp_jd_dwd', if_exists='replace',index=False)







savesql(df_saleShipping,'erp_jd_dwd','erp_jd_dwd_dim_saleshipping',"""CREATE TABLE `erp_jd_dwd_dim_saleshipping` (
  `riqi` datetime DEFAULT NULL,
  `wuliaobm` text,
  `wuliaomc` text,
  `shifasl` double DEFAULT NULL,
  `jiashuihj` double DEFAULT NULL,
  `hanshuidj` double DEFAULT NULL,
  `bumenbm` text,
  `bumenmc` text,
  `kehuid` text,
  `kehumc` text,
  `shifouzp` text,
  `shenhezt` text,
  `wuliaofzid` text,
  `wuliaofzmc` text,
  `cangkuid` text,
  `cangkumc` text,
  `shouhuofdz` text,
  `wuliaolbdm` text,
  `wuliaolbmc` text,
  `beizhu` text,
  `xiaoshoucbj` double DEFAULT NULL,
  `zongchengb` double DEFAULT NULL,
  `danjubh` text,
  `company` text,
  `bumen_new` text,
  `bumen` text,
  `year` text,
  `month` text,
  `cost` double DEFAULT NULL,
  `purchases` double DEFAULT NULL,
  `profit` double DEFAULT NULL,
  `refresh` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO erp_jd_dwd_dim_saleshipping(riqi,wuliaobm,wuliaomc,shifasl,jiashuihj,hanshuidj,bumenbm,bumenmc,kehuid,kehumc,shifouzp,shenhezt,wuliaofzid,wuliaofzmc,cangkuid,cangkumc,shouhuofdz,wuliaolbdm,wuliaolbmc,beizhu,xiaoshoucbj,zongchengb,danjubh,company,bumen_new,bumen,year,month,cost,purchases,profit,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")



savesql(df_client,'erp_jd_dwd','erp_jd_dwd_fact_client_c',"""CREATE TABLE `erp_jd_dwd_fact_client_c` (
  `fmasterid` bigint DEFAULT NULL,
  `kehubm` text,
  `kehumc` text,
  `kehufzid` text,
  `kehufzmc` text,
  `shenhezt` text,
  `chuangjiansj` text,
  `jiesuanfsid` text,
  `jiesuanfsmc` text,
  `shoukuantjid` text,
  `shoukuantjmc` text,
  `xiaoshoubmid` text,
  `xiaoshoubmmc` text,
  `company` text,
  `businessarea` text,
  `shouhuofdz` text,
  `name_prov1` text,
  `name_city1` text,
  `name_prov` text,
  `name_city` text,
  `name_coun` text,
  `refresh` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO erp_jd_dwd_fact_client_c(fmasterid,kehubm,kehumc,kehufzid,kehufzmc,shenhezt,chuangjiansj,jiesuanfsid,jiesuanfsmc,shoukuantjid,shoukuantjmc,xiaoshoubmid,xiaoshoubmmc,company,businessarea,shouhuofdz,name_prov1,name_city1,name_prov,name_city,name_coun,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")


savesql(df_client1,'erp_jd_dwd','erp_jd_dwd_fact_client',"""CREATE TABLE `erp_jd_dwd_fact_client` (
  `fmasterid` bigint DEFAULT NULL,
  `kehubm` text,
  `kehumc` text,
  `kehufzid` text,
  `kehufzmc` text,
  `shenhezt` text,
  `chuangjiansj` text,
  `jiesuanfsid` text,
  `jiesuanfsmc` text,
  `shoukuantjid` text,
  `shoukuantjmc` text,
  `xiaoshoubmid` text,
  `xiaoshoubmmc` text,
  `company` text,
  `businessarea` text,
  `shouhuofdz` text,
  `name_prov1` text,
  `name_city1` text,
  `name_prov` text,
  `name_city` text,
  `name_coun` text,
  `refresh` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO erp_jd_dwd_fact_client(fmasterid,kehubm,kehumc,kehufzid,kehufzmc,shenhezt,chuangjiansj,jiesuanfsid,jiesuanfsmc,shoukuantjid,shoukuantjmc,xiaoshoubmid,xiaoshoubmmc,company,businessarea,shouhuofdz,name_prov1,name_city1,name_prov,name_city,name_coun,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")






savesql(df_classify,'erp_jd_dwd','erp_jd_dwd_fact_classify',"""CREATE TABLE `erp_jd_dwd_fact_classify` (
  `wuliaobm` text,
  `wuliaomc` text,
  `wuliaofzid` text,
  `wuliaofzmc` text,
  `shenhezt` text,
  `chang` double DEFAULT NULL,
  `kuan` double DEFAULT NULL,
  `gao` double DEFAULT NULL,
  `danxiangbzsl` double DEFAULT NULL,
  `company` text,
  `wuliaofzid_2` text,
  `wuliaofzid_1` text,
  `classify` text,
  `classify_1` text,
  `wuliaofzid_3` text,
  `classify_2` text,
  `refresh` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO erp_jd_dwd_fact_classify(wuliaobm,wuliaomc,wuliaofzid,wuliaofzmc,shenhezt,chang,kuan,gao,danxiangbzsl,company,wuliaofzid_2,wuliaofzid_1,classify,classify_1,wuliaofzid_3,classify_2,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")



savesql(df_saleOrders,'erp_jd_dwd','erp_jd_dwd_dim_saleorders',"""CREATE TABLE `erp_jd_dwd_dim_saleorders` (
  `riqi` datetime DEFAULT NULL,
  `kehuid` text,
  `kehumc` text,
  `xiaoshoubmdm` text,
  `xiaoshoubmmc` text,
  `danjulxdm` text,
  `danjulxmc` text,
  `wuliaobm` text,
  `wuliaomc` text,
  `wuliaolbdm` text,
  `wuliaolbmc` text,
  `wuliaofzid` text,
  `wuliaofzmc` text,
  `jiashuihj` double DEFAULT NULL,
  `hanshuidj` double DEFAULT NULL,
  `xiaoshousl` double DEFAULT NULL,
  `leijicksl` double DEFAULT NULL,
  `leijithslxs` double DEFAULT NULL,
  `shouhuofdz` text,
  `cangkuid` text,
  `cangkumc` text,
  `shifouzp` text,
  `danjubh` text,
  `beizhu` text,  
  `company` text,
  `return_am` double DEFAULT NULL,
  `jiashuihj_ac` double DEFAULT NULL,
  `xiaoshousl_ac` double DEFAULT NULL,
  `bumen_new` text,
  `bumen` text,
  `year` text,
  `month` text,
  `cost` double DEFAULT NULL,
  `purchases` double DEFAULT NULL,
  `profit` double DEFAULT NULL,
  `purchases_ac` double DEFAULT NULL,
  `profit_ac` double DEFAULT NULL,
  `refresh` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO erp_jd_dwd_dim_saleorders(riqi,kehuid,kehumc,xiaoshoubmdm,xiaoshoubmmc,danjulxdm,danjulxmc,wuliaobm,wuliaomc,wuliaolbdm,wuliaolbmc,wuliaofzid,wuliaofzmc,jiashuihj,hanshuidj,xiaoshousl,leijicksl,leijithslxs,shouhuofdz,cangkuid,cangkumc,shifouzp,danjubh,beizhu,company,return_am,jiashuihj_ac,xiaoshousl_ac,bumen_new,bumen,year,month,cost,purchases,profit,purchases_ac,profit_ac,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")




savesql(df_saleReturn,'erp_jd_dwd','erp_jd_dwd_dim_salereturn',"""CREATE TABLE `erp_jd_dwd_dim_salereturn` (
  `riqi` datetime DEFAULT NULL,
  `wuliaobm` text,
  `wuliaomc` text,
  `shifasl` double DEFAULT NULL,
  `jiashuihj` double DEFAULT NULL,
  `hanshuidj` double DEFAULT NULL,
  `bumenbm` text,
  `bumenmc` text,
  `kehuid` text,
  `kehumc` text,
  `wuliaofzid` text,
  `wuliaofzmc` text,
  `shifouzp` text,
  `beizhu` text,
  `kucunzt` text,
  `shenhezt` text,
  `cangkuid` text,
  `cangkumc` text,
  `danjubh` text,
  `company` text,
  `bumen_new` text,
  `bumen` text,
  `year` text,
  `month` text,
  `cost` double DEFAULT NULL,
  `purchases` double DEFAULT NULL,
  `profit` double DEFAULT NULL,
  `refresh` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO erp_jd_dwd_dim_salereturn(riqi,wuliaobm,wuliaomc,shifasl,jiashuihj,hanshuidj,bumenbm,bumenmc,kehuid,kehumc,wuliaofzid,wuliaofzmc,shifouzp,beizhu,kucunzt,shenhezt,cangkuid,cangkumc,danjubh,company,bumen_new,bumen,year,month,cost,purchases,profit,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")




savesql(df_consignment,'erp_jd_dwd','erp_jd_dwd_dim_consignment',"""CREATE TABLE `erp_jd_dwd_dim_consignment` (
  `riqi` datetime DEFAULT NULL,
  `kehuid` text,
  `kehumc` text,
  `xiaoshoubmdm` text,
  `xiaoshoubmmc` text,
  `danjulxdm` text,
  `danjulxmc` text,
  `wuliaobm` text,
  `wuliaomc` text,
  `jiashuihj` double DEFAULT NULL,
  `shijijshsdj` double DEFAULT NULL,
  `hangbencijssl` double DEFAULT NULL,
  `shouhuofdz` text,
  `yuandiaorckid` text,
  `yuandiaorckmc` text,
  `yuandiaocckid` text,
  `yuandiaocckmc` text,
  `danjubh` text,
  `company` text,
  `bumen_new` text,
  `bumen` text,
  `year` text,
  `month` text,
  `cost` double DEFAULT NULL,
  `purchases` double DEFAULT NULL,
  `profit` double DEFAULT NULL,
  `refresh` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO erp_jd_dwd_dim_consignment(riqi,kehuid,kehumc,xiaoshoubmdm,xiaoshoubmmc,danjulxdm,danjulxmc,wuliaobm,wuliaomc,jiashuihj,shijijshsdj,hangbencijssl,shouhuofdz,yuandiaorckid,yuandiaorckmc,yuandiaocckid,yuandiaocckmc,danjubh,company,bumen_new,bumen,year,month,cost,purchases,profit,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")



savesql(df_purchasecost,'erp_jd_dwd','erp_jd_dwd_dim_purchasecost',"""CREATE TABLE `erp_jd_dwd_dim_purchasecost` (
  `wlmc_all` text,
  `year` text,
  `cost` double DEFAULT NULL,
  `refresh` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO erp_jd_dwd_dim_purchasecost(wlmc_all,year,cost,refresh) VALUES (%s,%s,%s,%s)")



savesql(df_purchaseOrders,'erp_jd_dwd','erp_jd_dwd_dim_purchaseorders',"""CREATE TABLE `erp_jd_dwd_dim_purchaseorders` (
  `riqi` datetime DEFAULT NULL,
  `jiashuihj` double DEFAULT NULL,
  `hanshuidj` double DEFAULT NULL,
  `caigousl` double DEFAULT NULL,
  `wuliaobm` text,
  `wuliaomc` text,
  `gongyingsid` text,
  `gongyingsmc` text,
  `shenhezt` text,
  `wuliaofzid` text,
  `wuliaofzmc` text,
  `caigoubmdm` text,
  `caigoubmmc` text,
  `chuangjianrid` text,
  `chuangjianrmc` text,
  `leijirksl` double DEFAULT NULL,
  `shengyurksl` double DEFAULT NULL,
  `leijitlsl` double DEFAULT NULL,
  `danjubh` text,
  `guanbizt` text,
  `danjia` double DEFAULT NULL,
  `shuilv` double DEFAULT NULL,
  `shuie` double DEFAULT NULL,
  `jine` double DEFAULT NULL,
  `company` text,
  `year` text,
  `month` text,
  `wlmc_all` text,
  `label` text,
  `caigousl_new` double DEFAULT NULL,
  `shifoucp` text,
  `mark_cp` text,
  `shengyurksl_new` double DEFAULT NULL,
  `leijirksl_new` double DEFAULT NULL,
  `hanshuidj_new` double DEFAULT NULL,
  `danjubh_1` text,
  `refresh` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO erp_jd_dwd_dim_purchaseorders(riqi,jiashuihj,hanshuidj,caigousl,wuliaobm,wuliaomc,gongyingsid,gongyingsmc,shenhezt,wuliaofzid,wuliaofzmc,caigoubmdm,caigoubmmc,chuangjianrid,chuangjianrmc,leijirksl,shengyurksl,leijitlsl,danjubh,guanbizt,danjia,shuilv,shuie,jine,company,year,month,wlmc_all,label,caigousl_new,shifoucp,mark_cp,shengyurksl_new,leijirksl_new,hanshuidj_new,danjubh_1,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")



savesql(df_purchaseReturn,'erp_jd_dwd','erp_jd_dwd_dim_purchasereturn',"""CREATE TABLE `erp_jd_dwd_dim_purchasereturn` (
  `tuiliaorq` datetime DEFAULT NULL,
  `gongyingsid` text,
  `gongyingsmc` text,
  `wuliaobm` text,
  `wuliaofzid` text,
  `wuliaofzmc` text,
  `cangkuid` text,
  `cangkumc` text,
  `shituisl` double DEFAULT NULL,
  `hanshuidj` double DEFAULT NULL,
  `jiashuihj` double DEFAULT NULL,
  `danjubh` text,
  `company` text,
  `year` text,
  `month` text,
  `wuliaomc` text,
  `wlmc_all` text,
  `label` text,
  `shituisl_new` double DEFAULT NULL,
  `shifoucp` text,
  `mark_cp` text,
  `refresh` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO erp_jd_dwd_dim_purchasereturn(tuiliaorq,gongyingsid,gongyingsmc,wuliaobm,wuliaofzid,wuliaofzmc,cangkuid,cangkumc,shituisl,hanshuidj,jiashuihj,danjubh,company,year,month,wuliaomc,wlmc_all,label,shituisl_new,shifoucp,mark_cp,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")



savesql(df_purchaseReceiving,'erp_jd_dwd','erp_jd_dwd_dim_purchasereceiving',"""CREATE TABLE `erp_jd_dwd_dim_purchasereceiving` (
  `riqi` datetime DEFAULT NULL,
  `wuliaobm` text,
  `wuliaomc` text,
  `gongyingsid` text,
  `gongyingsmc` text,
  `wuliaofzid` text,
  `wuliaofzmc` text,
  `cangkuid` text,
  `cangkumc` text,
  `shifasl` double DEFAULT NULL,
  `shenhezt` text,
  `danjubh` text,
  `dingdandh` text,
  `company` text,
  `year` text,
  `month` text,
  `wlmc_all` text,
  `label` text,
  `shifasl_new` double DEFAULT NULL,
  `shifoucp` text,
  `mark_cp` text,

  `refresh` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO erp_jd_dwd_dim_purchasereceiving(riqi,wuliaobm,wuliaomc,gongyingsid,gongyingsmc,wuliaofzid,wuliaofzmc,cangkuid,cangkumc,shifasl,shenhezt,danjubh,dingdandh,company,year,month,wlmc_all,label,shifasl_new,shifoucp,mark_cp,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")





print("\n","END DWD", datetime.now(),"\n")