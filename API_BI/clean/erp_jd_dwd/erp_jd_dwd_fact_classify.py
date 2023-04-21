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

engine.dispose()   

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

df_classify['refresh'] = datetime.now()


# *****************************************写入mysql*****************************************************# 
# df_classify.to_sql('erp_jd_dwd_fact_classify', engine1, schema='erp_jd_dwd', if_exists='replace',index=False) 




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
  `wuliaofzid_2` text,
  `wuliaofzid_1` text,
  `classify` text,
  `classify_1` text,
  `wuliaofzid_3` text,
  `classify_2` text,
  `refresh` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO erp_jd_dwd_fact_classify(wuliaobm,wuliaomc,wuliaofzid,wuliaofzmc,shenhezt,chang,kuan,gao,danxiangbzsl,tiaoma,company,wuliaofzid_2,wuliaofzid_1,classify,classify_1,wuliaofzid_3,classify_2,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
