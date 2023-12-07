# -*- coding: utf-8 -*-
# 测试环境: python3.9.6


# *****************************************自定义函数路径*************************************************#
import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

from key_tab import merge_label,cf,insertsql1
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine,text
import numpy as np


# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306')) 


# *****************************************取数据********************************************************#
finish_order      = pd.read_sql_query(text('select danjubh from localdata.finish_order;'), engine.connect())
df_voucherpayable = pd.read_sql_query(text('select caigouddh_1 from erp_jd_dwd.erp_jd_dwd_dim_voucherpayable;'), engine.connect())
df_saleShipping   = pd.read_sql_query(text('SELECT DISTINCT wuliaomc FROM `erp_jd_dwd`.`erp_jd_dwd_dim_saleshipping`;'), engine.connect())
df_wlys           = pd.read_sql_query(text('SELECT * FROM `erp_jd_dwd`.`erp_jd_dwd_fact_wuliaomc_ys`;'), engine.connect())

df_purchaseOrders = pd.read_sql_query(text(  """select * from erp_jd_ods.erp_jd_ods_dim_purchaseorders_wc_cwzx where gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司','上海卡卡丫文化传播有限公司','杭州游卡文化创意有限公司拱墅区分公司') and danjubh not in ('CGDD002945','CGDD002946','CGDD002947','CGDD002948','CGDD002949','CGDD002950','CGDD002951','CGDD002952','CGDD002953') and shenhezt = '已审核'
                                                union all 
                                                select * from erp_jd_ods.erp_jd_ods_dim_purchaseorders_ms_cwzx where gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司','上海卡卡丫文化传播有限公司','杭州游卡文化创意有限公司拱墅区分公司') and danjubh <> 'CGDD0005762' and shenhezt = '已审核'
                                                union all 
                                                select * from erp_jd_ods.erp_jd_ods_dim_purchaseorders_yc_cwzx where gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司','上海卡卡丫文化传播有限公司','杭州游卡文化创意有限公司拱墅区分公司') and shenhezt = '已审核'
                                                union all 
                                                select * from erp_jd_ods.erp_jd_ods_dim_purchaseorders_kyk_cwzx where gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司','上海卡卡丫文化传播有限公司','杭州游卡文化创意有限公司拱墅区分公司') and shenhezt = '已审核'
                                                union all 
                                                select * from erp_jd_ods.erp_jd_ods_dim_purchaseorders_wc01_cwzx where gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司','上海卡卡丫文化传播有限公司','杭州游卡文化创意有限公司拱墅区分公司') and shenhezt = '已审核';"""),   engine.connect()) 


engine.dispose()   


# ******************************************清洗表*******************************************************#
# df_purchaseOrders      采购订单表（新物料名称wlmc_all匹配为销售物料名称）
# ----------------------------------------------------------------------------------------------------- #
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
df_purchaseOrders = merge_label(df_purchaseOrders, df_saleShipping['wuliaomc'].to_list(),'caigousl','riqi',df_wlys)
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
    elif df_purchaseOrders['gongyingsmc'][i] in ['杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司','上海飞之火电竞信息科技有限公司','上海卡卡丫文化传播有限公司','杭州游卡文化创意有限公司拱墅区分公司']:
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


df_purchaseOrders.drop(['refresh_jk','fid','wlmc_new','yewugb'],axis=1,inplace = True)

# 修正物料名称
df_purchaseOrders = cf(df_purchaseOrders)
df_purchaseOrders['wlmc_all'] = df_purchaseOrders['wlmc_all'].map(lambda x:x.rstrip())


df_purchaseOrders['refresh'] = datetime.now()  


# *****************************************写入mysql*****************************************************#      
insertsql1( df_purchaseOrders,
            'erp_jd_dwd',
            'erp_jd_dwd_dim_purchaseorders',
            """INSERT INTO erp_jd_dwd_dim_purchaseorders(riqi,jiashuihj,hanshuidj,caigousl,wuliaobm,wuliaomc,gongyingsid,gongyingsmc,shenhezt,wuliaofzid,wuliaofzmc,caigoubmdm,caigoubmmc,chuangjianrid,chuangjianrmc,leijirksl,shengyurksl,leijitlsl,danjubh,guanbizt,danjia,shuilv,shuie,jine,company,shujuzx,year,month,wlmc_all,label,caigousl_new,shifoucp,mark_cp,shengyurksl_new,leijirksl_new,hanshuidj_new,danjubh_1,refresh) 
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""")
