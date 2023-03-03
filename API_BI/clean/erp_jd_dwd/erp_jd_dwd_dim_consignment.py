# -*- coding: utf-8 -*-
# 测试环境: python3.9.6


# *****************************************自定义函数路径*************************************************#
import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

from key_tab import savesql,getDictKey,clean,cf
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine


# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306')) 


# *****************************************取数据********************************************************#
df_purchasecost = pd.read_sql_query('select * from erp_jd_dwd.erp_jd_dwd_dim_purchasecost;', engine)
customer_name_change = pd.read_sql_query('select * from localdata.customer_name_change;', engine)
df_cost = pd.read_sql_query('SELECT * FROM `erp_jd_dwd`.`erp_jd_dwd_dim_cost`;',   engine) 

df_consignment = pd.read_sql_query("""select * from erp_jd_ods.erp_jd_ods_dim_consignment_wc_dobest where danjubh <> " "
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_consignment_wc_cwzx where danjubh <> " "
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_consignment_ms_cwzx where danjubh <> " ";""",   engine) 


df_classify = pd.read_sql_query('SELECT * FROM `erp_jd_dwd`.`erp_jd_dwd_fact_classify`;',   engine) 
df_saleShipping = pd.read_sql_query('SELECT DISTINCT wuliaomc FROM `erp_jd_dwd`.`erp_jd_dwd_dim_saleshipping`;',   engine) 
df_wlys = pd.read_sql_query('SELECT * FROM `erp_jd_dwd`.`erp_jd_dwd_fact_wuliaomc_ys`;',   engine) 

engine.dispose()   


# 物料字典
dict_cf = dict(zip(df_classify['wuliaobm'],df_classify['wuliaomc']))


customer_name_change['客户禁用抬头'] = customer_name_change['客户原抬头'].map(lambda x:x+'（禁用）')
dict_name_change = dict(zip(customer_name_change['客户'],list(zip(customer_name_change['客户禁用抬头'],customer_name_change['客户原抬头']))))


# df_consignment  寄售订单表
# ----------------------------------------------------------------------------------------------------- #
df_consignment = clean(df_consignment,df_cost,'xiaoshoubmmc','hangbencijssl')

# 客户原名称统一更改为新名称
df_consignment['kehumc'] = df_consignment['kehumc'].map(lambda x :getDictKey(dict_name_change,x,x))
df_consignment.drop(['refresh_jk','fid'],axis=1,inplace = True)

# 修正物料名称
df_consignment = cf(df_consignment,df_classify,dict_cf)

df_consignment['refresh'] = datetime.now()


# *****************************************写入mysql*****************************************************# 
# df_consignment.to_sql('erp_jd_dwd_dim_consignment', engine1, schema='erp_jd_dwd', if_exists='replace',index=False)
   


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
