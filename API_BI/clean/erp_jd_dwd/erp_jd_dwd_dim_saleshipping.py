# -*- coding: utf-8 -*-
# 测试环境: python3.9.6

import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

from key_tab import clean,getDictKey,cf,savesql,ct
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine


# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306'))  


# *****************************************取数据********************************************************#
township_area = pd.read_sql_query('select * from baidu_map.township_area;', engine)
df_purchasecost = pd.read_sql_query('select * from erp_jd_dwd.erp_jd_dwd_dim_purchasecost;', engine)


df_saleShipping = pd.read_sql_query("""select * from erp_jd_ods.erp_jd_ods_dim_saleshipping_wc_dobest where shenhezt <> '其他' and wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')
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
                                        select * from erp_jd_ods.erp_jd_ods_dim_saleshipping_kyk_cwzx where wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中');""",   engine) 



df_cost = pd.read_sql_query('SELECT * FROM `erp_jd_dwd`.`erp_jd_dwd_dim_cost`;',   engine) 

engine.dispose()   


# ******************************************清洗表*******************************************************#
# df_saleShipping  销售出库表
# ----------------------------------------------------------------------------------------------------- #
# 销售清洗，增加部门、成本、新品分类
df_saleShipping = clean(df_saleShipping,df_cost,'bumenmc','shifasl')
df_saleShipping.drop(['refresh_jk','fid'],axis=1,inplace = True)

# 客户原名称统一更改为新名称
df_saleShipping = ct(df_saleShipping)
# 修正物料名称
df_saleShipping = cf(df_saleShipping)

df_saleShipping['refresh'] = datetime.now()  

# *****************************************写入mysql*****************************************************# 
# df_saleShipping.to_sql('erp_jd_dwd_dim_saleshipping', engine, schema='erp_jd_dwd', if_exists='replace',index=False)
 
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
