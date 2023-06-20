# -*- coding: utf-8 -*-
# 测试环境: python3.9.6


# *****************************************自定义函数路径*************************************************#
import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

from key_tab import savesql
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine,text
import pymysql

# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306'))


# *****************************************取数据********************************************************#
df_classify = pd.read_sql_query(text("""select * from erp_jd_ods.erp_jd_ods_fact_classify_wc_dobest
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_fact_classify_wc_cwzx
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_fact_classify_ms_dobest
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_fact_classify_ms_cwzx
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_fact_classify_yc_xmgs
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_fact_classify_yc_cwzx
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_fact_classify_kyk_cwzx;"""),   engine.connect())

df_wuliaofzid   = pd.read_sql_query(text('select * from erp_jd_dwd.erp_jd_dwd_fact_wuliaofzid;'),   engine.connect())

# engine.dispose()   

# ******************************************清洗表*******************************************************#
# df_classify   物料分类表（区分欢乐坊、三国杀、yokakids、其他）
# ----------------------------------------------------------------------------------------------------- # 
# 先降序分组再去重
df_classify.sort_values(['wuliaofzid'],ascending=False ,inplace=True)
df_classify = df_classify[df_classify['wuliaomc'].duplicated()==False]

df_classify = pd.merge(df_classify ,df_wuliaofzid,left_on=['wuliaofzid'],right_on = ['wuliaofzid_3'],how='left')

df_classify1 = df_classify[df_classify['wuliaofzid_3'].isna()].drop(['wuliaofzid_0','wuliaofzmc_0','wuliaofzid_1','wuliaofzmc_1','wuliaofzid_2','wuliaofzmc_2','wuliaofzid_3','wuliaofzmc_3'],axis=1)
df_classify0 = df_classify[~df_classify['wuliaofzid_3'].isna()]
df_classify1  = pd.merge(df_classify1 ,df_wuliaofzid,left_on=['wuliaofzid'],right_on = ['wuliaofzid_2'],how='left')
df_classify11 = df_classify1[df_classify1['wuliaofzid_2'].isna()].drop(['wuliaofzid_0','wuliaofzmc_0','wuliaofzid_1','wuliaofzmc_1','wuliaofzid_2','wuliaofzmc_2','wuliaofzid_3','wuliaofzmc_3'],axis=1)
df_classify10 = df_classify1[~df_classify1['wuliaofzid_2'].isna()]
df_classify11  = pd.merge(df_classify11 ,df_wuliaofzid,left_on=['wuliaofzid'],right_on = ['wuliaofzid_1'],how='left')
df_classify111 = df_classify11[df_classify11['wuliaofzid_1'].isna()].drop(['wuliaofzid_0','wuliaofzmc_0','wuliaofzid_1','wuliaofzmc_1','wuliaofzid_2','wuliaofzmc_2','wuliaofzid_3','wuliaofzmc_3'],axis=1)
df_classify110 = df_classify11[~df_classify11['wuliaofzid_1'].isna()]
df_classify111  = pd.merge(df_classify111 ,df_wuliaofzid,left_on=['wuliaofzid'],right_on = ['wuliaofzid_0'],how='left')

df_classify = pd.concat([df_classify0,df_classify10,df_classify110,df_classify111])


df_classify.rename(columns={'wuliaofzmc_1':'classify', 'wuliaofzmc_2':'classify_1','wuliaofzmc_3':'classify_2'},inplace=True)


# 先降序分组再去重
df_classify.sort_values(['wuliaofzid'],ascending=False ,inplace=True)
df_classify = df_classify[df_classify['wuliaobm'].duplicated()==False]
df_classify = df_classify[df_classify['wuliaomc'].duplicated()==False]



df_classify.drop(['refresh_jk','fid'],axis=1,inplace = True)
df_classify['classify_2'].fillna(df_classify['classify_1'],inplace=True)


df_classify['refresh'] = datetime.now()


# *****************************************写入mysql*****************************************************# 
# df_classify.to_sql('erp_jd_dwd_fact_classify', engine, schema='erp_jd_dwd', if_exists='replace',index=False) 




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
  `tiaoma` text,
  `company` text,
  `wuliaofzid_0` text,
  `wuliaofzmc_0` text,
  `wuliaofzid_1` text,
  `classify` text,
  `wuliaofzid_2` text,
  `classify_1` text,
  `wuliaofzid_3` text,
  `classify_2` text,
  `refresh` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO erp_jd_dwd_fact_classify(wuliaobm,wuliaomc,wuliaofzid,wuliaofzmc,shenhezt,chang,kuan,gao,danxiangbzsl,tiaoma,company,wuliaofzid_0,wuliaofzmc_0,wuliaofzid_1,classify,wuliaofzid_2,classify_1,wuliaofzid_3,classify_2,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
