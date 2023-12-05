# -*- coding: utf-8 -*-
# 测试环境: python3.9.6

import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

from key_tab import clean,getDictKey,cf,ct,insertsql
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine,text


# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306'))  


# *****************************************取数据********************************************************#


df_saleShipping = pd.read_sql_query(text("""
                                        select * from erp_jd_ods.erp_jd_ods_dim_saleshipping_wc_cwzx 
                                        where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01') and wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')
                                        
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_saleshipping_ms_cwzx 
                                        where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01') and wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')
                                        
                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_saleshipping_yc_cwzx 
                                        where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01') and wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')

                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_saleshipping_kyk_cwzx 
                                        where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01') and wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')

                                        union all 
                                        select * from erp_jd_ods.erp_jd_ods_dim_saleshipping_wc01_cwzx 
                                        where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01') and wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中');"""),   engine.connect()) 


df_cost = pd.read_sql_query(text('SELECT * FROM `erp_jd_dwd`.`erp_jd_dwd_dim_cost`;'),                 engine.connect()) 

engine.dispose()   


# ******************************************清洗表*******************************************************#
# df_saleShipping  销售出库表
# ----------------------------------------------------------------------------------------------------- #
df_saleShipping.drop(['refresh_jk','fid'],axis=1,inplace = True)


# 销售清洗，增加部门、成本、新品分类
df_saleShipping = clean(df_saleShipping,df_cost,'bumenmc','shifasl')
# # 客户原名称统一更改为新名称
# df_saleShipping = ct(df_saleShipping)
# 修正物料名称
df_saleShipping = cf(df_saleShipping)


df_saleShipping['refresh'] = datetime.now()  

# *****************************************写入mysql*****************************************************# 
 
insertsql(  df_saleShipping,
            'erp_jd_dwd',
            'erp_jd_dwd_dim_saleshipping',
            """INSERT INTO erp_jd_dwd_dim_saleshipping(riqi,wuliaobm,wuliaomc,shifasl,jiashuihj,hanshuidj,bumenbm,bumenmc,kehuid,kehumc,shifouzp,shenhezt,wuliaofzid,wuliaofzmc,cangkuid,cangkumc,shouhuofdz,wuliaolbdm,wuliaolbmc,beizhu,xiaoshoucbj,zongchengb,danjubh,company,bumen_new,bumen,year,month,cost,purchases,profit,refresh) 
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            'riqi')