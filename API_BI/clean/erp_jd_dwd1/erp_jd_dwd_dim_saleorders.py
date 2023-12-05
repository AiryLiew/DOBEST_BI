  # -*- coding: utf-8 -*-
# 测试环境: python3.9.6


# *****************************************自定义函数路径*************************************************#
import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

from key_tab import clean,getDictKey,cf,insertsql1
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine,text


# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306')) 


# *****************************************取数据********************************************************#  
df_saleOrders = pd.read_sql_query(text("""
                                    select * from erp_jd_ods.erp_jd_ods_dim_saleorders_wc_cwzx where xiaoshoubmmc is not null
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_dim_saleorders_wc01_cwzx where xiaoshoubmmc is not null
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_dim_saleorders_ms_cwzx where xiaoshoubmmc is not null
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_dim_saleorders_yc_cwzx where xiaoshoubmmc is not null
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_dim_saleorders_kyk_cwzx where xiaoshoubmmc is not null;"""),   engine.connect())  


df_cost = pd.read_sql_query(text('SELECT * FROM `erp_jd_dwd`.`erp_jd_dwd_dim_cost`;'),   engine.connect())  
df_wlys = pd.read_sql_query(text('SELECT * FROM `erp_jd_dwd`.`erp_jd_dwd_fact_wuliaomc_ys`;'),   engine.connect())  

engine.dispose()   

# ******************************************清洗表*******************************************************#
# df_saleOrders  销售订单表
# ----------------------------------------------------------------------------------------------------- #
df_saleOrders.drop(['refresh_jk','fid'],axis=1,inplace = True)


df_saleOrders['return_am'] = df_saleOrders['hanshuidj']*df_saleOrders['leijithslxs']
df_saleOrders['jiashuihj_ac'] = df_saleOrders['jiashuihj'] - df_saleOrders['return_am']
df_saleOrders['xiaoshousl_ac'] = df_saleOrders['xiaoshousl'] - df_saleOrders['leijithslxs']

df_saleOrders = clean(df_saleOrders,df_cost,'xiaoshoubmmc','xiaoshousl')

df_saleOrders['purchases_ac'] = df_saleOrders['cost']*df_saleOrders['xiaoshousl_ac']
df_saleOrders['profit_ac'] = df_saleOrders['jiashuihj_ac']-df_saleOrders['purchases_ac']
df_saleOrders = df_saleOrders[~df_saleOrders['wuliaomc'].isin(['代收运费','测试物料1','管易云运费'])]

# 修正物料名称
df_saleOrders = cf(df_saleOrders)


df_saleOrders['refresh'] = datetime.now()

# *****************************************写入mysql*****************************************************# 

insertsql1( df_saleOrders,
			'erp_jd_dwd',
			'erp_jd_dwd_dim_saleorders',
			"""INSERT INTO erp_jd_dwd_dim_saleorders(riqi,kehuid,kehumc,xiaoshoubmdm,xiaoshoubmmc,danjulxdm,danjulxmc,wuliaobm,wuliaomc,wuliaolbdm,wuliaolbmc,wuliaofzid,wuliaofzmc,jiashuihj,hanshuidj,xiaoshousl,leijicksl,leijithslxs,shouhuofdz,cangkuid,cangkumc,shifouzp,danjubh,beizhu,sheng,shi,qu,company,shujuzx,return_am,jiashuihj_ac,xiaoshousl_ac,bumen_new,bumen,year,month,cost,purchases,profit,purchases_ac,profit_ac,refresh) 
			VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""")

