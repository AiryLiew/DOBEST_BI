# -*- coding: utf-8 -*-
# 测试环境: python3.9.6


import os
from datetime import datetime
print("\n","START DWD", datetime.now(),"\n")

  
folder_path = r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\erp_jd_dws1' 
folder_path1 = r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\erp_jd_dws2' 

def run(folder_path):
    python_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith(".py")]  

    for file_path in python_files:  
        os.system(f"python {file_path}")   


run(folder_path)
run(folder_path1)



# # *****************************************自定义函数路径*************************************************#
# import sys
# sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

# import pandas as pd
# from sqlalchemy import create_engine,text
# from datetime import datetime , timedelta
# from key_tab import savesql,getDictKey1,getDict

# print("\n","START DWS", datetime.now(),"\n")

# # *****************************************连接mysql、sql server*****************************************#
# engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306'))

        
# # *****************************************取数据********************************************************#
# df_closebalance      = pd.read_sql_query(text("select * from erp_jd_dws.erp_jd_dws_closebalance where 科目编码 = '1122.06' and 客户编码 is not null and 账簿 is not null ;"), engine.connect())
# dlzy_inventory       = pd.read_sql_query(text('select * from www_bi_ads.dlzy_inventory;'), engine.connect())
# df_purchasereceiving = pd.read_sql_query(text('select * from erp_jd_dwd.erp_jd_dwd_dim_purchasereceiving;'), engine.connect())
# df_beginninginventory= pd.read_sql_query(text("select * from erp_jd_dwd.erp_jd_dwd_dim_beginninginventory;"), engine.connect())
# # 其他入库路径的采购
# df_d = pd.read_sql_query(text("""SELECT  a.wuliaomc,a.riqi,sum(a.shishousl) amount FROM `erp_jd_dwd`.`erp_jd_dwd_dim_othersreceiving` a
#                                 LEFT JOIN (
#                                 SELECT DISTINCT wlmc_all wuliaomc FROM `erp_jd_dwd`.`erp_jd_dwd_dim_purchasereceiving`
#                                 union
#                                 SELECT DISTINCT wuliaomc FROM `erp_jd_dwd`.`erp_jd_dwd_dim_purchasereceiving`
#                                 union
#                                 SELECT DISTINCT wuliaomc FROM `erp_jd_dwd`.`erp_jd_dwd_dim_beginninginventory`
#                                 ) b on a.wuliaomc = b.wuliaomc
#                                 where b.wuliaomc is null
#                                 GROUP BY a.wuliaomc,a.riqi;"""), engine.connect())


# df_warehouse= pd.read_sql_query(text("""select riqi,wuliaomc,cangkumc,cangkuid,case when wuliaomc is not null then 0 end receiving,CONVERT(pankuisl,SIGNED) shipping,company,case when wuliaomc is not null then '盘亏单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_inventoryloss where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
#                                     union all
#                                     select riqi,wuliaomc,cangkumc,cangkuid,CONVERT(panyingsl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '盘盈单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_inventoryprofit where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
#                                     union all
#                                     select riqi,wuliaomc,cangkumc,cangkuid,CONVERT(shishousl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '其他入库单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_othersreceiving where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
#                                     union all
#                                     select riqi,wuliaomc,cangkumc,cangkuid,case when wuliaomc is not null then 0 end receiving,CONVERT(shifasl,SIGNED) shipping,company,case when wuliaomc is not null then '其他出库单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_othersshipping where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') and shenhezt = '已审核'

#                                     union all
#                                     select riqi,wuliaomc,cangkumc,cangkuid,case when wuliaomc is not null then 0 end receiving,CONVERT(-shifasl,SIGNED) shipping,company,case when wuliaomc is not null then '销售退货单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_salereturn where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') and shenhezt = '已审核'
#                                     union all
#                                     select riqi,wuliaomc,cangkumc,cangkuid,case when wuliaomc is not null then 0 end receiving,CONVERT(shifasl,SIGNED) shipping,company,case when wuliaomc is not null then '销售出库单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_saleshipping where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') and shenhezt = '已审核'

#                                     union all
#                                     select rukurq riqi,wuliaomc,cangkumc,cangkuid,CONVERT(shuliang,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '组装拆卸单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_assemble where shiwulx in ('组装','拆卸子件') and wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') 
#                                     union all 
#                                     select rukurq riqi,wuliaomc,cangkumc,cangkuid,case when wuliaomc is not null then 0 end receiving,CONVERT(shuliang,SIGNED) shipping,company,case when wuliaomc is not null then '组装拆卸单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_assemble where shiwulx in ('拆卸','组装子件') and wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') 


#                                     union all
#                                     select riqi,wuliaomc,diaoruck cangkumc,diaoruckid cangkuid,CONVERT(diaobosl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '直接调拨单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_allocation where diaobofx <> '退货' and wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') and danjuzt = '已审核'
#                                     union all 
#                                     select riqi,wuliaomc,diaoruck cangkumc,diaoruckid cangkuid,case when wuliaomc is not null then 0 end receiving,CONVERT(-diaobosl,SIGNED) shipping,company,case when wuliaomc is not null then '直接调拨单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_allocation where diaobofx = '退货' and wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') and danjuzt = '已审核'
#                                     union all 
#                                     select riqi,wuliaomc,diaochuck cangkumc,diaochuckid cangkuid,case when wuliaomc is not null then 0 end receiving,CONVERT(diaobosl,SIGNED) shipping,company,case when wuliaomc is not null then '直接调拨单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_allocation where diaobofx <> '退货' and wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') and danjuzt = '已审核'
#                                     union all 
#                                     select riqi,wuliaomc,diaochuck cangkumc,diaochuckid cangkuid,CONVERT(-diaobosl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '直接调拨单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_allocation where diaobofx = '退货' and wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') and danjuzt = '已审核'

#                                     union all
#                                     select riqi,wuliaomc,diaochuck cangkumc,diaochuckid cangkuid,CONVERT(-diaorusl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '分布式调入单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_distributedin where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') and danjuzt = '已审核'
#                                     union all 
#                                     select riqi,wuliaomc,diaoruck cangkumc,diaoruckid cangkuid,CONVERT(diaorusl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '分布式调入单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_distributedin where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') and danjuzt = '已审核'

#                                     union all
#                                     select riqi,wuliaomc,diaochuck cangkumc,diaochuckid cangkuid,CONVERT(diaochusl,SIGNED) receiving,CONVERT(diaochusl,SIGNED) shipping,company,case when wuliaomc is not null then '分布式调出单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_distributedout where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') and danjuzt = '已审核'

#                                     union all
                                
#                                     select 
#                                     a.`riqi`,
#                                     case when b.wuliaomc is null then a.wuliaomc else b.wuliaomc end wuliaomc,
#                                     a.cangkumc,
#                                     a.cangkuid,
#                                     a.receiving,
#                                     a.shipping,
#                                     a.company,
#                                     a.`table` 
#                                     from (
#                                     select tuiliaorq riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(-shituisl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '采购退料单' end `table` from erp_jd_ods.erp_jd_ods_dim_purchasereturn_wc_dobest where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
#                                     union all 
#                                     select tuiliaorq riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(-shituisl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '采购退料单' end `table` from erp_jd_ods.erp_jd_ods_dim_purchasereturn_wc_cwzx where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
#                                     union all 
#                                     select tuiliaorq riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(-shituisl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '采购退料单' end `table` from erp_jd_ods.erp_jd_ods_dim_purchasereturn_ms_cwzx where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
#                                     union all 
#                                     select tuiliaorq riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(-shituisl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '采购退料单' end `table` from erp_jd_ods.erp_jd_ods_dim_purchasereturn_yc_xmgs where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
#                                     union all 
#                                     select tuiliaorq riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(-shituisl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '采购退料单' end `table` from erp_jd_ods.erp_jd_ods_dim_purchasereturn_yc_cwzx where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
#                                     union all 
#                                     select tuiliaorq riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(-shituisl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '采购退料单' end `table` from erp_jd_ods.erp_jd_ods_dim_purchasereturn_kyk_cwzx  where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')

#                                     union all
#                                     select riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(shifasl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '采购入库单' end `table` from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_wc_dobest where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
#                                     union all
#                                     select riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(shifasl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '采购入库单' end `table` from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_wc_cwzx where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
#                                     union all 
#                                     select riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(shifasl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '采购入库单' end `table` from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_ms_dobest where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
#                                     union all 
#                                     select riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(shifasl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '采购入库单' end `table` from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_ms_cwzx where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
#                                     union all 
#                                     select riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(shifasl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '采购入库单' end `table` from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_yc_xmgs where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
#                                     union all 
#                                     select riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(shifasl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '采购入库单' end `table` from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_yc_cwzx where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
#                                     union all 
#                                     select riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(shifasl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '采购入库单' end `table` from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_kyk_cwzx where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
#                                     union all 
#                                     select riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(shifasl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '采购入库单' end `table` from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_wc01_cwzx where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')                                    
                                    
                                    
#                                     union all
#                                     select riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(receiving,SIGNED) receiving,CONVERT(shipping,SIGNED) shipping,company,`table` from erp_jd_dwd.erp_jd_dwd_dim_beginninginventory
#                                     ) a
#                                     LEFT JOIN(
#                                     SELECT wuliaobm,wuliaomc FROM `erp_jd_dwd`.`erp_jd_dwd_fact_classify`
#                                     ) b on a.wuliaobm = b.wuliaobm;"""), engine.connect())


# engine.dispose()





# # df_warehouse   库存表
# # ----------------------------------------------------------------------------------------------------- # 
# df_warehouse['inventory'] = df_warehouse['receiving'] - df_warehouse['shipping']

# df_warehouse.sort_values(['wuliaomc','riqi','receiving', 'shipping','company', 'table'],ascending=[True,True,False,False,False,False],inplace=True,ignore_index=True)
# df_warehouse1 = df_warehouse.set_index(['riqi','wuliaomc']).groupby(['wuliaomc'])['inventory'].cumsum().reset_index().rename(columns = {'inventory':'inventory_wl'})
# df_warehouse = pd.concat([df_warehouse,df_warehouse1['inventory_wl']],axis=1)


# df_warehouse['refresh'] = datetime.now()

# savesql(df_warehouse,'erp_jd_dws','erp_jd_dws_warehouse',"""CREATE TABLE `erp_jd_dws_warehouse` (
#   `riqi` datetime DEFAULT NULL,
#   `wuliaomc` text,
#   `cangkumc` text,
#   `cangkuid` text,
#   `shipping` double DEFAULT NULL,
#   `company` text,
#   `table` text,
#   `receiving` double DEFAULT NULL,
#   `inventory` double DEFAULT NULL,
#   `inventory_wl` double DEFAULT NULL,
#   `refresh` datetime DEFAULT NULL
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
# "INSERT INTO erp_jd_dws_warehouse(riqi,wuliaomc,cangkumc,cangkuid,receiving,shipping,company,`table`,inventory,inventory_wl,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")





# # df_warehouse_dayend  日库存结余（含月末）
# # ----------------------------------------------------------------------------------------------------- # 
# def dayend(df_warehouse):
#     df_warehouse_dayend = df_warehouse.groupby(['wuliaomc','riqi'],as_index=False).agg({'receiving':'sum','shipping':'sum'})
#     df_warehouse_dayend['inventory'] = df_warehouse_dayend['receiving']-df_warehouse_dayend['shipping']

#     def funckc(df_warehouse,name):    
#         df_warehouse.sort_values(['wuliaomc','riqi'],ascending=True,inplace=True,ignore_index=True)
#         listWL = []
#         for i in df_warehouse['wuliaomc'].drop_duplicates():
#             dfW = df_warehouse[df_warehouse['wuliaomc'] == i]
#             dfW.loc[:,name] = dfW['inventory'].cumsum()
#             listWL.append(dfW)
#         return pd.concat(listWL,ignore_index=True)

#     df_warehouse_dayend = funckc(df_warehouse_dayend,'inventory_wl')


#     df_warehouse_dayend['riqi'] = pd.to_datetime(df_warehouse_dayend['riqi'],format = '%Y-%m-%d')
#     # 插入日期
#     dateMax = df_warehouse_dayend['riqi'].max()
    
#     df_warehouse_dayend = df_warehouse_dayend.join(df_warehouse_dayend.groupby(['wuliaomc'],as_index=False)['riqi'].rank(ascending=False).rename(columns={'riqi':'rank'}))
#     a = df_warehouse_dayend[(df_warehouse_dayend['rank']==1)&(df_warehouse_dayend['inventory_wl']>0)&(df_warehouse_dayend['riqi']!=dateMax)]
#     a.loc[:,'riqi'] = dateMax
#     df_warehouse_dayend = pd.concat([df_warehouse_dayend,a],ignore_index=True)
#     df_warehouse_dayend.drop(['rank'],axis=1,inplace=True)


#     df_warehouse_dayend['year_month'] = df_warehouse_dayend['riqi'].map(lambda x :str(x)[:7])
#     dfindex = df_warehouse_dayend[['wuliaomc','year_month']].drop_duplicates(keep='last')
#     dfindex.loc[:,'ismonthend'] = '是'
#     df_warehouse_dayend = df_warehouse_dayend.join(dfindex['ismonthend'])

#     return df_warehouse_dayend




# # df_warehouse_ck_dayend  日库存结余（含月末）
# # ----------------------------------------------------------------------------------------------------- # 
# def dayend_ck(df_warehouse):
#     df_warehouse_dayend = df_warehouse.groupby(['wuliaomc','cangkumc','riqi'],as_index=False).agg({'receiving':'sum','shipping':'sum'})
#     df_warehouse_dayend['inventory'] = df_warehouse_dayend['receiving']-df_warehouse_dayend['shipping']

#     def funckc(df_warehouse,name):    
#         df_warehouse.sort_values(['wuliaomc','cangkumc','riqi'],ascending=True,inplace=True,ignore_index=True)
#         listWL = []
#         for i in df_warehouse['wuliaomc'].drop_duplicates():
#             dfW = df_warehouse[df_warehouse['wuliaomc'] == i]
#             for j in dfW['cangkumc'].drop_duplicates():
#                 dfW1 = dfW[dfW['cangkumc'] == j]
#                 dfW1.loc[:,name] = dfW1['inventory'].cumsum()
#                 listWL.append(dfW1)
#         return pd.concat(listWL,ignore_index=True)

#     df_warehouse_dayend = funckc(df_warehouse_dayend,'inventory_wl')



#     df_warehouse_dayend['riqi'] = pd.to_datetime(df_warehouse_dayend['riqi'],format = '%Y-%m-%d')
#     # 插入日期
#     dateMax = df_warehouse_dayend['riqi'].max()

#     df_warehouse_dayend = df_warehouse_dayend.join(df_warehouse_dayend.groupby(['wuliaomc','cangkumc'],as_index=False)['riqi'].rank(ascending=False).rename(columns={'riqi':'rank'}))
#     a = df_warehouse_dayend[(df_warehouse_dayend['rank']==1)&(df_warehouse_dayend['inventory_wl']>0)&(df_warehouse_dayend['riqi']!=dateMax)]
#     a.loc[:,'riqi'] = dateMax
#     df_warehouse_dayend = pd.concat([df_warehouse_dayend,a],ignore_index=True)
#     df_warehouse_dayend.drop(['rank'],axis=1,inplace=True)



#     df_warehouse_dayend['year_month'] = df_warehouse_dayend['riqi'].map(lambda x :str(x)[:7])
#     dfindex = df_warehouse_dayend[['wuliaomc','cangkumc','year_month']].drop_duplicates(keep='last')
#     dfindex.loc[:,'ismonthend'] = '是'
#     df_warehouse_dayend = df_warehouse_dayend.join(dfindex['ismonthend'])

#     return df_warehouse_dayend


# # 调用

# df_warehouse_ck_dayend = dayend_ck(df_warehouse)
# dlzy_inventory_ck_dayend= dayend_ck(dlzy_inventory)


# df_warehouse_dayend = dayend(df_warehouse)
# dlzy_inventory_dayend = dayend(dlzy_inventory)
# dlzy_inventory_dayend = pd.merge(dlzy_inventory_dayend,dlzy_inventory[['属性','wuliaomc']].drop_duplicates(),on=['wuliaomc'],how='left')








# # df_doi   库存天数表
# # ----------------------------------------------------------------------------------------------------- # 
# def funcA(df_warehouse,df_purchasereceiving,date,df_d = df_d):  
#     a = df_warehouse.groupby('wuliaomc',as_index=False)['inventory'].sum()
#     df_purchasereceiving.loc[:,'shifasl_new'] = df_purchasereceiving['shifasl_new'].astype(float)

#     b = df_purchasereceiving.groupby(['wlmc_all','riqi'],as_index=False)['shifasl_new'].sum()
#     b = b[b['shifasl_new']!= 0]

#     c = df_beginninginventory[['wuliaomc','riqi','receiving']]
#     c.rename(columns={'receiving':'amount'},inplace=True)
#     b.rename(columns={'wlmc_all':'wuliaomc','shifasl_new':'amount'},inplace=True)
#     d = pd.concat([c,b,df_d],ignore_index=True)


    
#     d['amount'] = d['amount'].astype(int)
#     d['riqi'] = d['riqi'].map(lambda x:str(x)[:10])
#     d['riqi'] = pd.to_datetime(d['riqi'],format='%Y-%m-%d')

#     # 增加库存 
#     mydict = dict(zip(a['wuliaomc'],a['inventory']))
#     list_ = []         
#     for i in d['wuliaomc'].drop_duplicates():
#         b1 = d[d['wuliaomc']==i]
#         b1.sort_values(['riqi'],ascending=False,inplace=True,ignore_index=True)
#         b1.reset_index(drop = True,inplace = True)
#         # 累计采购及期初数量
#         b1.loc[:,'t_amount'] = b1['amount'].cumsum()

#         for j in range(len(b1)):
#             if getDictKey1(mydict,b1['wuliaomc'][j],0)<=b1['t_amount'][j]:
#                 list_.append(b1.loc[:j])
#                 break
#     df1 = pd.concat(list_,ignore_index=True)

#     df2 = pd.merge(df1,a,on=['wuliaomc'],how='left')
#     df2['inventory'].fillna(0,inplace = True)
#     df2.dropna(subset=['wuliaomc'],inplace=True)

#     df2['surplus'] = pd.Series([], dtype='float64')
#     list_js = []
#     for i in df2['wuliaomc'].drop_duplicates():
#         b2 = df2[df2['wuliaomc']==i]
#         b2.reset_index(drop = True,inplace = True)
#         if len(b2)==1:
#             b2['surplus'].fillna(b2['inventory'],inplace = True)
#             list_js.append(b2)
#         else: 
#             b2['surplus'].fillna(b2['amount'],inplace = True)
#             b2.loc[b2.index[-1],'surplus'] = b2['inventory'][-1:].values[0]- b2['amount'][:-1].sum()
#             list_js.append(b2)
#     df_doi = pd.concat(list_js,ignore_index=True)

#     df_doi['riqi'] = df_doi['riqi'].map(lambda x:str(x)[:10])
#     df_doi['riqi'] = pd.to_datetime(df_doi['riqi'],format="%Y-%m-%d")

#     df_doi['doi'] = date - df_doi['riqi']
#     df_doi['doi'] = df_doi['doi'].map(lambda x:int(str(x).split(' ')[0]) if str(x) != 'NaT' else x)

#     for i in range(len(df_doi)):
#         if df_doi['inventory'][i]<=0:
#             df_doi.loc[i,'doi'] = 0


#     # dict_level = {
#     #             '0-30天':[0,30],
#     #             '30-90天':[30,90],
#     #             '90-180天':[90,180],
#     #             '180-360天':[180,360],
#     #             '360天以上':[360,10000]}

#     dict_level = {
#                 '0-180天':[-1,180],
#                 '180-360天':[180,360],
#                 '360天以上':[360,10000]}
    
#     df_doi['level'] = df_doi['doi'].map(lambda x:getDict(dict_level,x,'无库存'))
#     # for i in range(len(df_doi)):
#     #     if df_doi['surplus'][i] == 0:
#     #         df_doi['level'][i] = '无库存'
#     df_doi = df_doi[df_doi['surplus']!=0]

#     return df_doi

# df_doi = funcA(df_warehouse,df_purchasereceiving,datetime.today())


# # df_warehouse['riqi'] = pd.to_datetime(df_warehouse['riqi'],format='%Y-%m-%d')
# # df_purchasereceiving['riqi'] = pd.to_datetime(df_purchasereceiving['riqi'],format='%Y-%m-%d')

# # a30 = df_warehouse[df_warehouse['riqi']<=datetime.today()-timedelta(30)]
# # b30 = df_purchasereceiving[df_purchasereceiving['riqi']<=datetime.today()-timedelta(30)]
# # df_doi_30 = funcA(a30,b30,datetime.today()-timedelta(30))


# # a60 = df_warehouse[df_warehouse['riqi']<=datetime.today()-timedelta(60)]
# # b60 = df_purchasereceiving[df_purchasereceiving['riqi']<=datetime.today()-timedelta(60)]
# # df_doi_60 = funcA(a60,b60,datetime.today()-timedelta(60))


# # a7 = df_warehouse[df_warehouse['riqi']<=datetime.today()-timedelta(7)]
# # b7 = df_purchasereceiving[df_purchasereceiving['riqi']<=datetime.today()-timedelta(7)]
# # df_doi_7 = funcA(a7,b7,datetime.today()-timedelta(7))





# # df_ageofreceivables   账龄表
# # ----------------------------------------------------------------------------------------------------- # 
# df_closebalance.fillna(0,inplace=True)


# list_ = []
# for j in df_closebalance['账簿'].drop_duplicates():
#     for i in df_closebalance['客户名称'].drop_duplicates():
#         dfcb_01 = df_closebalance[(df_closebalance['账簿']==j)&(df_closebalance['客户名称']==i)]
#         dfcb_01.sort_values(['日期','审核状态'],ascending=True,inplace=True)
#         dfcb_01.reset_index(drop=True,inplace=True)
#         try:
#             dfcb_01 = dfcb_01.iloc[dfcb_01[dfcb_01['审核状态'] == '期初'].index[0]:]
#             list_.append(dfcb_01)
#         except:
#             list_.append(dfcb_01)

# df_cb = pd.concat(list_,ignore_index=True)

# # 期末余额
# df_end = df_cb.groupby(['账簿','客户名称','客户编码'],as_index=False).agg({'借方金额':'sum','贷方金额':'sum'})
# df_end ['余额'] = df_end ['借方金额']-df_end['贷方金额']

# # 借贷明细
# df_mx = df_cb.groupby(['账簿','客户名称','客户编码','日期'],as_index=False).agg({'借方金额':'sum','贷方金额':'sum'})
# df_mx['rank'] = df_mx.groupby(['账簿','客户名称','客户编码'])['日期'].rank(ascending=True, method='first') 

# # 处理借贷方向
# df_mx['借方金额1'] = df_mx['借方金额']
# df_mx['贷方金额1'] = df_mx['贷方金额']
# for i in range(len(df_mx)):
#     if df_mx['借方金额1'][i]<0:
#         df_mx.loc[i,'贷方金额1'] = df_mx['贷方金额1'][i]-df_mx['借方金额1'][i]
#         df_mx.loc[i,'借方金额1'] = 0
#     elif df_mx['贷方金额1'][i]<0:
#         df_mx.loc[i,'借方金额1'] = df_mx['借方金额1'][i]-df_mx['贷方金额1'][i]
#         df_mx.loc[i,'贷方金额1'] = 0   


# df_mx['余额'] = df_mx['借方金额1']-df_mx['贷方金额1']
# df_mx['累计余额'] = df_mx.groupby(['账簿','客户名称','客户编码'])['余额'].cumsum()
# df_mx['余额方向'] = df_mx['累计余额'].map(lambda x:"借" if x>0 else ("平" if x==0 else "贷"))
# df_mx['借方累计余额'] = df_mx.groupby(['账簿','客户名称','客户编码'])['借方金额1'].cumsum()
# df_mx['贷方累计余额'] = df_mx.groupby(['账簿','客户名称','客户编码'])['贷方金额1'].cumsum()
# # 标记最近一次发生日
# list_b = []
# for j in df_mx['账簿'].drop_duplicates():
#     for i in df_mx['客户编码'].drop_duplicates():
#         b = df_mx[(df_mx['客户编码']==i)&(df_mx['账簿']==j)]
#         list_b.append(b[b['rank']==b['rank'].max()])
# df_markend = pd.concat(list_b)

# # 连接
# df_markend.rename(columns={'日期':'最后一次发生日期'},inplace=True)
# df_mx = df_mx.join(df_markend['最后一次发生日期'])




# # 计算每笔结余
# def surplus(df_mx):
#     list_ab = []
#     for j in df_mx['账簿'].drop_duplicates():
#         for i in df_mx['客户编码'].drop_duplicates():
            
#             a = df_mx[(df_mx['客户编码']==i)&(df_mx['账簿']==j)]
#             try:
#                 if list(a['余额方向'])[-1] == '借':
#                     # 贷方金额总合计
#                     b = a['贷方金额1'].sum()
#                     # 冲借方金额
#                     list_lj = []
#                     a.reset_index(drop=True,inplace=True)
#                     for i in range(len(a)):
#                         if b >= a['借方金额1'][i]:
#                             list_lj.append(0)
#                             b = b-a['借方金额1'][i]
#                         elif b < a['借方金额1'][i] and b != 0:
#                             list_lj.append(a['借方金额1'][i]-b)
#                             b = 0
#                         else:
#                             list_lj.append(a['借方金额1'][i])
#                     a.loc[:,'结余金额'] = pd.DataFrame(list_lj)
#                     list_ab.append(a)
                
#                 elif list(a['余额方向'])[-1] == '贷':
#                     # 借方金额总合计
#                     b = a['借方金额1'].sum()
#                     # 冲贷方金额
#                     list_lj = []
#                     a.reset_index(drop=True,inplace=True)
#                     for i in range(len(a)):
#                         if b >= a['贷方金额1'][i]:
#                             list_lj.append(0)
#                             b = b-a['贷方金额1'][i]
#                         elif b < a['贷方金额1'][i] and b != 0:
#                             list_lj.append(a['贷方金额1'][i]-b)
#                             b = 0
#                         else:
#                             list_lj.append(a['贷方金额1'][i])
#                     a.loc[:,'结余金额'] = pd.DataFrame(list_lj)
#                     list_ab.append(a)
#             except:
#                 pass
#     df = pd.concat(list_ab)
#     return df


# df_mx1 = surplus(df_mx)


# df_mx1['日期'] = pd.to_datetime(df_mx1['日期'],format='%Y-%m-%d')
# df_mx1['账龄'] = datetime.now()-df_mx1['日期']
# df_mx1['账龄'] = df_mx1['账龄'].map(lambda x:int(str(x).split(' ')[0]) if str(x) != 'NaT' else x)

# df_ageofreceivables = df_mx1[df_mx1['结余金额']>0][['账簿','客户名称','客户编码','日期','借方金额','贷方金额','余额','余额方向','结余金额','账龄']]
# dict_label = {
#                 '0-30天':[0,30],
#                 '31-60天':[30,60],
#                 '61-90天':[60,90],
#                 '91-180天':[90,180],
#                 '181-360天':[180,360],
#                 '360天以上':[360,10000]}

# df_ageofreceivables['账龄区间'] = df_ageofreceivables['账龄'].map(lambda x:getDict(dict_label,x,'无'))





# # df_doi_fc  分仓库存天数表
# # ----------------------------------------------------------------------------------------------------- # 
# med = df_warehouse.groupby(['wuliaomc'],as_index=False).agg({'inventory':'sum'})
# df_warehouse = pd.merge(df_warehouse,med.rename(columns={'inventory':'med'}) ,on=['wuliaomc'],how='left')
# df_warehouse = df_warehouse[df_warehouse['med']>0].drop(['med'],axis = 1)
# med1 = df_warehouse.groupby(['wuliaomc','cangkumc'],as_index=False).agg({'inventory':'sum'})
# df_warehouse = pd.merge(df_warehouse,med1.rename(columns={'inventory':'med'}) ,on=['wuliaomc','cangkumc'],how='left')
# df_warehouse = df_warehouse[df_warehouse['med']>0].drop(['med'],axis = 1)


# # 排序计算累计各仓库存
# df_warehouse.sort_values(['wuliaomc','riqi','receiving'],ascending=[True,True,False],inplace=True)
# b = df_warehouse.groupby(['wuliaomc','cangkumc','riqi'],as_index=False).agg({'receiving':'sum','shipping':'sum'})
# b['inventory'] = b['receiving'] - b['shipping'] 
# b['inventory_add'] = b.groupby(['wuliaomc','cangkumc'])['inventory'].cumsum()

# # 去除库存为0数据，匹配当前库存列
# c = b[b['inventory_add']>0]
# list_index = c.groupby(['wuliaomc','cangkumc'])['riqi'].idxmax().to_list()
# d = c.loc[list_index][['wuliaomc','cangkumc','inventory_add']].rename(columns={'inventory_add':'inventory'})
# c = pd.merge(c[['wuliaomc','cangkumc','riqi','receiving']],d,on=['wuliaomc','cangkumc'],how='left')
# d = c[c['receiving']>0]
# d.sort_values(['wuliaomc','cangkumc','riqi'],ascending=False,inplace=True)
# d.loc[:,'receiving_add'] = d.groupby(['wuliaomc','cangkumc'])['receiving'].cumsum()

# # 辅助列判断当前库存是否大于倒算累计入库
# d.loc[:,'fz'] = d['receiving_add']-d['inventory']
# d.loc[:,'fz'] = d['fz'].map(lambda x: 'y' if x >= 0 else 'n')
# # 截小于入库的连接去重后的大于入库的
# d_n = d[d['fz']=='n']
# d_y = d[d['fz']=='y'].drop_duplicates(['wuliaomc','cangkumc'])
# e = pd.concat([d_n,d_y]).sort_values(['wuliaomc','cangkumc','riqi'],ascending=False)

# # 各仓库存天数
# e['surplus'] = pd.Series([], dtype='float64')
# list_e1 = []
# for i in e['wuliaomc'].drop_duplicates():
#     for j in e[e['wuliaomc']==i]['cangkumc'].drop_duplicates():
#         e1 = e[(e['wuliaomc']==i)&(e['cangkumc']==j)].reset_index(drop = True)
#         if len(e1)==1:
#             e1['surplus'].fillna(e1['inventory'],inplace = True)
#             list_e1.append(e1)
#         else: 
#             e1['surplus'].fillna(e1['receiving'],inplace = True)
#             e1.loc[e1.index[-1],'surplus'] = e1['inventory'][-1:].values[0]- e1['receiving'][:-1].sum()
#             list_e1.append(e1)
# df_doi_fc = pd.concat(list_e1,ignore_index=True)

# df_doi_fc['riqi'] = df_doi_fc['riqi'].map(lambda x:str(x)[:10])
# df_doi_fc['riqi'] = pd.to_datetime(df_doi_fc['riqi'],format="%Y-%m-%d")

# df_doi_fc['doi'] = datetime.today() - df_doi_fc['riqi']
# df_doi_fc['doi'] = df_doi_fc['doi'].map(lambda x:int(str(x).split(' ')[0]) if str(x) != 'NaT' else x)

# for i in range(len(df_doi_fc)):
#     if df_doi_fc['inventory'][i]<=0:
#         df_doi_fc.loc[i,'doi'] = 0


# df_doi_fc['riqi'] = df_doi_fc['riqi'].map(lambda x:str(x)[:10])
# df_doi_fc['riqi'] = pd.to_datetime(df_doi_fc['riqi'],format="%Y-%m-%d")

# df_doi_fc['doi'] = datetime.today() - df_doi_fc['riqi']
# df_doi_fc['doi'] = df_doi_fc['doi'].map(lambda x:int(str(x).split(' ')[0]) if str(x) != 'NaT' else x)

# for i in range(len(df_doi_fc)):
#     if df_doi_fc['inventory'][i]<=0:
#         df_doi_fc.loc[i,'doi'] = 0

# dict_level = {
#             '0-30天':[-1,30],
#             '30-90天':[30,90],
#             '90-180天':[90,180],
#             '180-360天':[180,360],
#             '360天以上':[360,10000]}

# df_doi_fc['level'] = df_doi_fc['doi'].map(lambda x:getDict(dict_level,x,'无库存'))



# # *****************************************数据更新增加*****************************************************# 
# df_doi['refresh'] = datetime.now()
# df_doi_fc['refresh'] = datetime.now()
# # df_doi_7['refresh'] = datetime.now()
# # df_doi_30['refresh'] = datetime.now()
# # df_doi_60['refresh'] = datetime.now()
# df_ageofreceivables['refresh'] = datetime.now()


# # *****************************************写入mysql*****************************************************#

# savesql(df_warehouse_ck_dayend,'erp_jd_dws','erp_jd_dws_warehouse_ck_dayend',"""CREATE TABLE `erp_jd_dws_warehouse_ck_dayend` (
#   `wuliaomc` text,
#   `cangkumc` text,
#   `riqi` datetime DEFAULT NULL,
#   `receiving` double DEFAULT NULL,
#   `shipping` double DEFAULT NULL,
#   `inventory` double DEFAULT NULL,
#   `inventory_wl` double DEFAULT NULL,
#   `year_month` text,
#   `ismonthend` text
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
# "INSERT INTO erp_jd_dws_warehouse_ck_dayend(wuliaomc,cangkumc,riqi,receiving,shipping,inventory,inventory_wl,`year_month`,`ismonthend`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)")


# savesql(df_warehouse_dayend,'erp_jd_dws','erp_jd_dws_warehouse_dayend',"""CREATE TABLE `erp_jd_dws_warehouse_dayend` (
#   `wuliaomc` text,
#   `riqi` datetime DEFAULT NULL,
#   `receiving` double DEFAULT NULL,
#   `shipping` double DEFAULT NULL,
#   `inventory` double DEFAULT NULL,
#   `inventory_wl` double DEFAULT NULL,
#   `year_month` text,
#   `ismonthend` text
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
# "INSERT INTO erp_jd_dws_warehouse_dayend(wuliaomc,riqi,receiving,shipping,inventory,inventory_wl,`year_month`,`ismonthend`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)")




# savesql(dlzy_inventory_dayend,'www_bi_ads','dlzy_inventory_dayend',"""CREATE TABLE `dlzy_inventory_dayend` (
#   `wuliaomc` text,
#   `riqi` datetime DEFAULT NULL,
#   `receiving` double DEFAULT NULL,
#   `shipping` double DEFAULT NULL,
#   `inventory` double DEFAULT NULL,
#   `inventory_wl` double DEFAULT NULL,
#   `year_month` text,
#   `ismonthend` text,
#   `属性` text
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
# "INSERT INTO dlzy_inventory_dayend(wuliaomc,riqi,receiving,shipping,inventory,inventory_wl,`year_month`,`ismonthend`,`属性`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)")


# savesql(dlzy_inventory_ck_dayend,'www_bi_ads','dlzy_inventory_ck_dayend',"""CREATE TABLE `dlzy_inventory_ck_dayend` (
#   `wuliaomc` text,
#   `cangkumc` text,
#   `riqi` datetime DEFAULT NULL,
#   `receiving` double DEFAULT NULL,
#   `shipping` double DEFAULT NULL,
#   `inventory` double DEFAULT NULL,
#   `inventory_wl` double DEFAULT NULL,
#   `year_month` text,
#   `ismonthend` text
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
# "INSERT INTO dlzy_inventory_ck_dayend(wuliaomc,cangkumc,riqi,receiving,shipping,inventory,inventory_wl,`year_month`,`ismonthend`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)")





# savesql(df_doi,'erp_jd_dws','erp_jd_dws_doi',"""CREATE TABLE `erp_jd_dws_doi` (
#   `wuliaomc` text,
#   `riqi` datetime DEFAULT NULL,
#   `amount` int DEFAULT NULL,
#   `t_amount` int DEFAULT NULL,
#   `inventory` double DEFAULT NULL,
#   `surplus` double DEFAULT NULL,
#   `doi` bigint DEFAULT NULL,
#   `level` text,
#   `refresh` datetime DEFAULT NULL
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
# "INSERT INTO erp_jd_dws_doi(wuliaomc,riqi,amount,t_amount,inventory,surplus,doi,level,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)")



# savesql(df_doi_fc,'erp_jd_dws','erp_jd_dws_doi_fc',"""CREATE TABLE `erp_jd_dws_doi_fc` (
#   `wuliaomc` text,
#   `cangkumc` text,
#   `riqi` datetime DEFAULT NULL,
#   `receiving` double DEFAULT NULL,
#   `inventory` double DEFAULT NULL,
#   `receiving_add` double DEFAULT NULL,
#   `fz` text,
#   `surplus` double DEFAULT NULL,
#   `doi` bigint DEFAULT NULL,
#   `level` text,
#   `refresh` datetime DEFAULT NULL
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
# "INSERT INTO erp_jd_dws_doi_fc(wuliaomc,cangkumc,riqi,receiving,inventory,receiving_add,fz,surplus,doi,level,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")



# # savesql(df_doi_7,'erp_jd_dws','erp_jd_dws_doi_lastweek',"""CREATE TABLE `erp_jd_dws_doi_lastweek` (
# #   `wuliaomc` text,
# #   `riqi` datetime DEFAULT NULL,
# #   `amount` int DEFAULT NULL,
# #   `t_amount` int DEFAULT NULL,
# #   `inventory` double DEFAULT NULL,
# #   `surplus` double DEFAULT NULL,
# #   `doi` bigint DEFAULT NULL,
# #   `level` text,
# #   `refresh` datetime DEFAULT NULL
# # ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
# # "INSERT INTO erp_jd_dws_doi_lastweek(wuliaomc,riqi,amount,t_amount,inventory,surplus,doi,level,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)")



# # savesql(df_doi_30,'erp_jd_dws','erp_jd_dws_doi_lastmonth',"""CREATE TABLE `erp_jd_dws_doi_lastmonth` (
# #   `wuliaomc` text,
# #   `riqi` datetime DEFAULT NULL,
# #   `amount` int DEFAULT NULL,
# #   `t_amount` int DEFAULT NULL,
# #   `inventory` double DEFAULT NULL,
# #   `surplus` double DEFAULT NULL,
# #   `doi` bigint DEFAULT NULL,
# #   `level` text,
# #   `refresh` datetime DEFAULT NULL
# # ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
# # "INSERT INTO erp_jd_dws_doi_lastmonth(wuliaomc,riqi,amount,t_amount,inventory,surplus,doi,level,refresh)VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)")


# # savesql(df_doi_60,'erp_jd_dws','erp_jd_dws_doi_last2month',"""CREATE TABLE `erp_jd_dws_doi_last2month` (
# #   `wuliaomc` text,
# #   `riqi` datetime DEFAULT NULL,
# #   `amount` int DEFAULT NULL,
# #   `t_amount` int DEFAULT NULL,
# #   `inventory` double DEFAULT NULL,
# #   `surplus` double DEFAULT NULL,
# #   `doi` bigint DEFAULT NULL,
# #   `level` text,
# #   `refresh` datetime DEFAULT NULL
# # ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;""",
# # "INSERT INTO erp_jd_dws_doi_last2month(wuliaomc,riqi,amount,t_amount,inventory,surplus,doi,level,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)")


# savesql(df_ageofreceivables,'erp_jd_ads','erp_jd_ads_ageofreceivables_1122_06',"""CREATE TABLE `erp_jd_ads_ageofreceivables_1122_06` (
#   `账簿` text,
#   `客户名称` text,
#   `客户编码` text,
#   `日期` datetime DEFAULT NULL,
#   `借方金额` double DEFAULT NULL,
#   `贷方金额` double DEFAULT NULL,
#   `余额` double DEFAULT NULL,
#   `余额方向` text,
#   `结余金额` double DEFAULT NULL,
#   `账龄` bigint DEFAULT NULL,
#   `账龄区间` text,
#   `refresh` datetime DEFAULT NULL
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;""",
# "INSERT INTO erp_jd_ads_ageofreceivables_1122_06(账簿,客户名称,客户编码,日期,借方金额,贷方金额,余额,余额方向,结余金额,账龄,账龄区间,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")




print("\n","END DWS", datetime.now(),"\n")