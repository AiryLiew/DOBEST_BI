# -*- coding: utf-8 -*-
# 测试环境: python3.9.6


from datetime import datetime
print("\n","START erp_jd_dwd_fact_client", datetime.now(),"\n")


# *****************************************自定义函数路径*************************************************#
import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

from key_tab import savesql,area,getDictKey
from sqlalchemy import create_engine
import pandas as pd


# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306')) 


# *****************************************取数据********************************************************#
df_client = pd.read_sql_query("""select * from erp_jd_ods.erp_jd_ods_fact_client_wc_dobest where shenhezt = '已审核'
                                union all 
                                select * from erp_jd_ods.erp_jd_ods_fact_client_wc_cwzx where shenhezt = '已审核'
                                union all 
                                select * from erp_jd_ods.erp_jd_ods_fact_client_ms_dobest where shenhezt = '已审核'
                                union all
                                select * from erp_jd_ods.erp_jd_ods_fact_client_ms_cwzx where shenhezt = '已审核'
                                union all 
                                select * from erp_jd_ods.erp_jd_ods_fact_client_yc_xmgs where shenhezt = '已审核'
                                union all 
                                select * from erp_jd_ods.erp_jd_ods_fact_client_yc_cwzx where shenhezt = '已审核'
                                union all 
                                select * from erp_jd_ods.erp_jd_ods_fact_client_kyk_cwzx where shenhezt = '已审核';""",   engine) 
df_saleOrders        = pd.read_sql_query('select * from erp_jd_dwd.erp_jd_dwd_dim_saleorders;',       engine)


engine.dispose()   

# ******************************************清洗表*******************************************************#

# df_client   区域表
# ----------------------------------------------------------------------------------------------------- # 
dict_client = {
    '零售事业组':['便利','百货','超市','连锁','书店','母婴系统','教育系统','新零售'],  
    '批发流通事业组':['东北','华北','华东','华南','华中','西南','西北'] 
    }
df_client['businessarea'] = df_client['kehufzmc'].map(lambda x :getDictKey(dict_client,x,'其他'))
df_client['kehumc'] = df_client['kehumc'].map(lambda x:x.rsplit()[0])

# 客户地区表
df_customerarea = df_saleOrders[['kehumc','shouhuofdz']].drop_duplicates()
df_client = pd.merge(df_client,df_customerarea,on=['kehumc'],how='left')
# 增加省市
df_client = area(df_client)
df_client.sort_values(['kehumc','chuangjiansj','kehufzmc'],ascending=False,inplace=True)
df_client1 = df_client[df_client.duplicated('kehumc')==False]

df_client = df_client.drop(['name_coun1','refresh_jk','fid'],axis = 1)
df_client1 = df_client1.drop(['name_coun1','refresh_jk','fid'],axis = 1)

df_client['refresh'] = datetime.now()
df_client1['refresh'] = datetime.now()

# *****************************************写入mysql*****************************************************# 
# df_client.to_sql('erp_jd_dwd_fact_client',engine, schema='erp_jd_dwd', if_exists='replace',index=False) 


savesql(df_client1,'erp_jd_dwd','erp_jd_dwd_fact_client',"""CREATE TABLE `erp_jd_dwd_fact_client` (
  `fmasterid` bigint DEFAULT NULL,
  `kehubm` text,
  `kehumc` text,
  `kehufzid` text,
  `kehufzmc` text,
  `shenhezt` text,
  `chuangjiansj` text,
  `jiesuanfsid` text,
  `jiesuanfsmc` text,
  `shoukuantjid` text,
  `shoukuantjmc` text,
  `xiaoshoubmid` text,
  `xiaoshoubmmc` text,
  `company` text,
  `businessarea` text,
  `shouhuofdz` text,
  `name_prov1` text,
  `name_city1` text,
  `name_prov` text,
  `name_city` text,
  `name_coun` text,
  `refresh` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO erp_jd_dwd_fact_client(fmasterid,kehubm,kehumc,kehufzid,kehufzmc,shenhezt,chuangjiansj,jiesuanfsid,jiesuanfsmc,shoukuantjid,shoukuantjmc,xiaoshoubmid,xiaoshoubmmc,company,businessarea,shouhuofdz,name_prov1,name_city1,name_prov,name_city,name_coun,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")

savesql(df_client,'erp_jd_dwd','erp_jd_dwd_fact_client_c',"""CREATE TABLE `erp_jd_dwd_fact_client_c` (
  `fmasterid` bigint DEFAULT NULL,
  `kehubm` text,
  `kehumc` text,
  `kehufzid` text,
  `kehufzmc` text,
  `shenhezt` text,
  `chuangjiansj` text,
  `jiesuanfsid` text,
  `jiesuanfsmc` text,
  `shoukuantjid` text,
  `shoukuantjmc` text,
  `xiaoshoubmid` text,
  `xiaoshoubmmc` text,
  `company` text,
  `businessarea` text,
  `shouhuofdz` text,
  `name_prov1` text,
  `name_city1` text,
  `name_prov` text,
  `name_city` text,
  `name_coun` text,
  `refresh` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO erp_jd_dwd_fact_client_c(fmasterid,kehubm,kehumc,kehufzid,kehufzmc,shenhezt,chuangjiansj,jiesuanfsid,jiesuanfsmc,shoukuantjid,shoukuantjmc,xiaoshoubmid,xiaoshoubmmc,company,businessarea,shouhuofdz,name_prov1,name_city1,name_prov,name_city,name_coun,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")


print("\n","END erp_jd_dwd_fact_client", datetime.now(),"\n")
