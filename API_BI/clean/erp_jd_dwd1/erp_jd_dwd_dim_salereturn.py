# -*- coding: utf-8 -*-
# 测试环境: python3.9.6


# *****************************************自定义函数路径*************************************************#
import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

from key_tab import clean,getDictKey,cf,insertsql
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine,text


# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306')) 


# *****************************************取数据********************************************************#
df_saleReturn = pd.read_sql_query(text(""" 
                                    select * from erp_jd_ods.erp_jd_ods_dim_salereturn_wc_cwzx 
                                    where  riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01') and wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')

                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_dim_salereturn_yc_cwzx 
                                    where  riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01') and wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')
                                    
									union all 
                                    select * from erp_jd_ods.erp_jd_ods_dim_salereturn_kyk_cwzx 
                                    where  riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01') and wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中')
                                    
									union all 
                                    select * from erp_jd_ods.erp_jd_ods_dim_salereturn_wc01_cwzx 
                                    where  riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01') and wuliaomc not in ('代收运费','测试物料1','管易云运费') and shenhezt in ('已审核','其他','审核中');"""),  engine.connect()) 


df_cost = pd.read_sql_query(text('SELECT * FROM `erp_jd_dwd`.`erp_jd_dwd_dim_cost`;'),   engine.connect()) 

engine.dispose()   

          
# df_saleReturn  销售退货表
# ----------------------------------------------------------------------------------------------------- #
df_saleReturn.drop(['refresh_jk','fid'],axis=1,inplace = True)
# 销售清洗，增加部门、成本、新品分类
df_saleReturn = clean(df_saleReturn,df_cost,'bumenmc','shifasl')

df_saleReturn['refresh'] = datetime.now()


# *****************************************写入mysql*****************************************************# 
insertsql(  df_saleReturn,
			'erp_jd_dwd',
			'erp_jd_dwd_dim_salereturn',
			"""INSERT INTO erp_jd_dwd_dim_salereturn(riqi,wuliaobm,wuliaomc,shifasl,jiashuihj,hanshuidj,bumenbm,bumenmc,kehuid,kehumc,wuliaofzid,wuliaofzmc,shifouzp,beizhu,kucunzt,shenhezt,cangkuid,cangkumc,danjubh,company,bumen_new,bumen,year,month,cost,purchases,profit,refresh) 
			VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
			'riqi')

