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
df_classify = pd.read_sql_query(text("""select * from erp_jd_ods.erp_jd_ods_fact_classify_wc_cwzx where shenhezt = '已审核' and wuliaomc not in ('收入调整','管易云运费','测试物料1','对接用')
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_fact_classify_ms_cwzx where shenhezt = '已审核' and wuliaomc not in ('收入调整','管易云运费','测试物料1','对接用')
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_fact_classify_yc_cwzx where shenhezt = '已审核' and wuliaomc not in ('收入调整','管易云运费','测试物料1','对接用')
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_fact_classify_kyk_cwzx where shenhezt = '已审核' and wuliaomc not in ('收入调整','管易云运费','测试物料1','对接用')
                                    union all 
                                    select * from erp_jd_ods.erp_jd_ods_fact_classify_wc01_cwzx where shenhezt = '已审核' and wuliaomc not in ('收入调整','管易云运费','测试物料1','对接用');"""),   engine.connect())


engine.dispose()   

df_classify.drop(['refresh_jk'],axis=1,inplace = True)


df_classify['name_group'] = df_classify['wuliaofzmc'].map(lambda x:x.split('-'))
df_classify['id_group'] = df_classify['wuliaofzid'].map(lambda x:[str(x)[0:i+2] for i in range(0, len(str(x)), 2)])

# 展开列为多列  
df_classify[['wuliaofzmc_0','classify','classify_1','classify_2','wuliaofzmc_4']] = df_classify['name_group'].apply(pd.Series)  
df_classify[['wuliaofzid_0','wuliaofzid_1','wuliaofzid_2','wuliaofzid_3','wuliaofzid_4']] = df_classify['id_group'].apply(pd.Series)  

# 删除原始列  
df_classify.drop(columns=['name_group','id_group','wuliaofzmc_4','wuliaofzid_4'], inplace=True)  


# 先降序分组再去重
df_classify.sort_values(['wuliaofzid','wuliaofzid_0'],ascending=False ,inplace=True)
df_classify = df_classify[df_classify['wuliaobm'].duplicated()==False]
df_classify = df_classify[df_classify['wuliaomc'].duplicated()==False]


df_classify['refresh'] = datetime.now()


# *****************************************写入mysql*****************************************************# 
savesql(df_classify,'erp_jd_dwd','erp_jd_dwd_fact_classify',"""CREATE TABLE `erp_jd_dwd_fact_classify` (
  `fid` text,
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
  `shujuzx` text,
  `wuliaofzmc_0` text,
  `classify` text,
  `classify_1` text,
  `classify_2` text,
  `wuliaofzid_0` text,
  `wuliaofzid_1` text,
  `wuliaofzid_2` text,
  `wuliaofzid_3` text,
  `refresh` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
"INSERT INTO erp_jd_dwd_fact_classify(fid,wuliaobm,wuliaomc,wuliaofzid,wuliaofzmc,shenhezt,chang,kuan,gao,danxiangbzsl,tiaoma,company,shujuzx,wuliaofzmc_0, classify, classify_1, classify_2,wuliaofzid_0, wuliaofzid_1, wuliaofzid_2, wuliaofzid_3,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")