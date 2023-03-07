# -*- coding: utf-8 -*-
# 测试环境: python3.9.6


# *****************************************自定义函数路径*************************************************#
import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

from key_tab import merge_label,savesql,cf
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine,text


# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306')) 
# conn = create_engine("mssql+pymssql://{}:{}@{}:{}/{}".format('sa', '123456', '10.242.21.1', '1433', 'erp_jd_dwd'))


# *****************************************取数据********************************************************#
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
                                        select * from erp_jd_ods.erp_jd_ods_dim_purchasereturn_kyk_cwzx;"""),   engine.connect())  

df_cost = pd.read_sql_query(text('SELECT * FROM `erp_jd_dwd`.`erp_jd_dwd_dim_cost`;'),   engine.connect())  
df_wlys = pd.read_sql_query(text('SELECT * FROM `erp_jd_dwd`.`erp_jd_dwd_fact_wuliaomc_ys`;'),   engine.connect())  
df_saleShipping = pd.read_sql_query(text('SELECT DISTINCT wuliaomc FROM `erp_jd_dwd`.`erp_jd_dwd_dim_saleshipping`;'),   engine.connect())  
engine.dispose()   


# ******************************************清洗表*******************************************************#
# df_purchaseReturn   采购退料表
# ----------------------------------------------------------------------------------------------------- #
df_purchaseReturn = merge_label(df_purchaseReturn,df_saleShipping, 'shituisl','tuiliaorq',df_wlys)
df_purchaseReturn.drop(['refresh_jk','fid','wlmc_new'],axis=1,inplace = True)

# 修正物料名称
df_purchaseReturn = cf(df_purchaseReturn)

df_purchaseReturn['refresh'] = datetime.now()

# *****************************************写入mysql*****************************************************#   
# df_purchaseReturn.to_sql('erp_jd_dwd_dim_purchasereturn',engine, schema='erp_jd_dwd', if_exists='replace',index=False) 


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
