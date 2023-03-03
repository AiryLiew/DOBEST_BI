# -*- coding: utf-8 -*-
# 测试环境: python3.9.6


# *****************************************自定义函数路径*************************************************#
import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

from key_tab import clean,getDictKey,cf,savesql
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine


# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306')) 


# *****************************************取数据********************************************************#
df_purchasecost = pd.read_sql_query('select * from erp_jd_dwd.erp_jd_dwd_dim_purchasecost;', engine)
customer_name_change = pd.read_sql_query('select * from localdata.customer_name_change;', engine)

df_saleReturn = pd.read_sql_query("""select * from erp_jd_ods.erp_jd_ods_dim_salereturn_wc_dobest where wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_dim_salereturn_wc_cwzx where wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_dim_salereturn_yc_xmgs where wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_dim_salereturn_yc_cwzx where wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_dim_salereturn_ms_dobest where wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_dim_salereturn_kyk_cwzx where wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中');""",   engine) 


df_classify = pd.read_sql_query('SELECT * FROM `erp_jd_dwd`.`erp_jd_dwd_fact_classify`;',   engine) 
df_cost = pd.read_sql_query('SELECT * FROM `erp_jd_dwd`.`erp_jd_dwd_dim_cost`;',   engine) 
df_wlys = pd.read_sql_query('SELECT * FROM `erp_jd_dwd`.`erp_jd_dwd_fact_wuliaomc_ys`;',   engine) 

engine.dispose()   


# 物料字典
dict_cf = dict(zip(df_classify['wuliaobm'],df_classify['wuliaomc']))

customer_name_change['客户禁用抬头'] = customer_name_change['客户原抬头'].map(lambda x:x+'（禁用）')
dict_name_change = dict(zip(customer_name_change['客户'],list(zip(customer_name_change['客户禁用抬头'],customer_name_change['客户原抬头']))))

          
# df_saleReturn  销售退货表
# ----------------------------------------------------------------------------------------------------- #
# 销售清洗，增加部门、成本、新品分类
df_saleReturn = clean(df_saleReturn,df_cost,'bumenmc','shifasl')

# 客户原名称统一更改为新名称
df_saleReturn['kehumc'] = df_saleReturn['kehumc'].map(lambda x :getDictKey(dict_name_change,x,x))
df_saleReturn.drop(['refresh_jk','fid'],axis=1,inplace = True)

# 修正物料名称
df_saleReturn = cf(df_saleReturn,df_classify,dict_cf)


df_saleReturn['refresh'] = datetime.now()


# *****************************************写入mysql*****************************************************# 
# df_saleReturn.to_sql('erp_jd_dwd_dim_salereturn', engine1, schema='erp_jd_dwd', if_exists='replace',index=False)


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

