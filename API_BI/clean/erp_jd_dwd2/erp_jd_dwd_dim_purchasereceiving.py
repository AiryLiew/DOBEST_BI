# -*- coding: utf-8 -*-
# 测试环境: python3.9.6


# *****************************************自定义函数路径*************************************************#
import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

from key_tab import merge_label,insertsql,cf
import pandas as pd
from sqlalchemy import create_engine,text
from datetime import datetime


# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306')) 


# *****************************************取数据********************************************************#
df_purchaseReceiving = pd.read_sql_query(text("""
                                            select * from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_wc_cwzx 
                                            where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01') and gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司','上海卡卡丫文化传播有限公司','杭州游卡文化创意有限公司拱墅区分公司')
                                            
											union all 
                                            select * from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_ms_cwzx 
											where  riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01') and gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司','上海卡卡丫文化传播有限公司','杭州游卡文化创意有限公司拱墅区分公司')
                                            
											union all 
                                            select * from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_yc_cwzx 
											where  riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01') and gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司','上海卡卡丫文化传播有限公司','杭州游卡文化创意有限公司拱墅区分公司')
                                            
											union all 
                                            select * from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_kyk_cwzx 
											where  riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01') and gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司','上海卡卡丫文化传播有限公司','杭州游卡文化创意有限公司拱墅区分公司')
                                            
											union all 
                                            select * from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_wc01_cwzx 
											where  riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01') and gongyingsmc not in ('杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司','上海卡卡丫文化传播有限公司','杭州游卡文化创意有限公司拱墅区分公司');"""),   engine.connect()) 


df_saleShipping = pd.read_sql_query(text('SELECT DISTINCT wuliaomc FROM `erp_jd_dwd`.`erp_jd_dwd_dim_saleshipping`;'),   engine.connect()) 
df_wlys = pd.read_sql_query(text('SELECT * FROM `erp_jd_dwd`.`erp_jd_dwd_fact_wuliaomc_ys`;'),   engine.connect()) 

engine.dispose()   


# df_purchaseReceiving   采购入库表
# ----------------------------------------------------------------------------------------------------- #
# 修正物料名称
df_purchaseReceiving = cf(df_purchaseReceiving)

df_purchaseReceiving['shifasl'] = df_purchaseReceiving['shifasl'].astype(float)
df_purchaseReceiving = merge_label(df_purchaseReceiving,df_saleShipping['wuliaomc'].drop_duplicates().to_list(), 'shifasl','riqi',df_wlys)
# 空物料名称增补
df_purchaseReceiving['wlmc_all'].fillna(df_purchaseReceiving['wuliaomc'],inplace=True)

df_purchaseReceiving.reset_index(drop=True,inplace=True)
df_purchaseReceiving.drop(['refresh_jk','fid','wlmc_new'],axis=1,inplace = True)




df_purchaseReceiving['refresh'] = datetime.now() 


# *****************************************写入mysql*****************************************************# 
insertsql(  df_purchaseReceiving,
            'erp_jd_dwd',
            'erp_jd_dwd_dim_purchasereceiving',
            """INSERT INTO erp_jd_dwd_dim_purchasereceiving(riqi,wuliaobm,wuliaomc,gongyingsid,gongyingsmc,wuliaofzid,wuliaofzmc,cangkuid,cangkumc,shifasl,shenhezt,danjubh,dingdandh,company,year,month,wlmc_all,label,shifasl_new,shifoucp,mark_cp,refresh) 
			VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
			'riqi')
