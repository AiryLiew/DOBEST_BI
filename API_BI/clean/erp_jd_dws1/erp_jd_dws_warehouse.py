# -*- coding: utf-8 -*-
# 测试环境: python3.9.6


# *****************************************自定义函数路径*************************************************#
import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

import pandas as pd
from sqlalchemy import create_engine,text
from datetime import datetime
from key_tab import insertsql

print("\n","START DWS", datetime.now(),"\n")

# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306'))

        
# *****************************************取数据********************************************************#


df_warehouse= pd.read_sql_query(text("""select riqi,wuliaomc,cangkumc,cangkuid,case when wuliaomc is not null then 0 end receiving,CONVERT(pankuisl,SIGNED) shipping,company,case when wuliaomc is not null then '盘亏单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_inventoryloss 
                                    where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
                                    and riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

                                    
									union all
                                    select riqi,wuliaomc,cangkumc,cangkuid,CONVERT(panyingsl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '盘盈单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_inventoryprofit 
                                    where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
                                    and riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

                                    
									union all
                                    select riqi,wuliaomc,cangkumc,cangkuid,CONVERT(shishousl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '其他入库单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_othersreceiving 
                                    where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
                                    and riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

                                    
									union all
                                    select riqi,wuliaomc,cangkumc,cangkuid,case when wuliaomc is not null then 0 end receiving,CONVERT(shifasl,SIGNED) shipping,company,case when wuliaomc is not null then '其他出库单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_othersshipping 
									where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') and shenhezt = '已审核'
                                    and riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')


                                    union all
                                    select riqi,wuliaomc,cangkumc,cangkuid,case when wuliaomc is not null then 0 end receiving,CONVERT(-shifasl,SIGNED) shipping,company,case when wuliaomc is not null then '销售退货单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_salereturn 
									where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') and shenhezt = '已审核'
                                    and riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

                                    
									union all
                                    select riqi,wuliaomc,cangkumc,cangkuid,case when wuliaomc is not null then 0 end receiving,CONVERT(shifasl,SIGNED) shipping,company,case when wuliaomc is not null then '销售出库单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_saleshipping 
									where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') and shenhezt = '已审核'
                                    and riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')


                                    union all
                                    select rukurq riqi,wuliaomc,cangkumc,cangkuid,CONVERT(shuliang,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '组装拆卸单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_assemble 
									where shiwulx in ('组装','拆卸子件') and wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') 
                                    and rukurq>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

                                    
									union all 
                                    select rukurq riqi,wuliaomc,cangkumc,cangkuid,case when wuliaomc is not null then 0 end receiving,CONVERT(shuliang,SIGNED) shipping,company,case when wuliaomc is not null then '组装拆卸单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_assemble 
									where shiwulx in ('拆卸','组装子件') and wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') 
                                    and rukurq>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')


                                    union all
                                    select riqi,wuliaomc,diaoruck cangkumc,diaoruckid cangkuid,CONVERT(diaobosl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '直接调拨单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_allocation 
									where diaobofx <> '退货' and wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') and danjuzt = '已审核'
                                    and riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

                                    union all 
                                    select riqi,wuliaomc,diaoruck cangkumc,diaoruckid cangkuid,case when wuliaomc is not null then 0 end receiving,CONVERT(-diaobosl,SIGNED) shipping,company,case when wuliaomc is not null then '直接调拨单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_allocation 
									where diaobofx = '退货' and wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') and danjuzt = '已审核'
                                    and riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

                                    union all 
                                    select riqi,wuliaomc,diaochuck cangkumc,diaochuckid cangkuid,case when wuliaomc is not null then 0 end receiving,CONVERT(diaobosl,SIGNED) shipping,company,case when wuliaomc is not null then '直接调拨单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_allocation 
									where diaobofx <> '退货' and wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') and danjuzt = '已审核'
                                    and riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

                                    union all 
                                    select riqi,wuliaomc,diaochuck cangkumc,diaochuckid cangkuid,CONVERT(-diaobosl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '直接调拨单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_allocation 
									where diaobofx = '退货' and wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') and danjuzt = '已审核'
                                    and riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')


                                    union all
                                    select riqi,wuliaomc,diaochuck cangkumc,diaochuckid cangkuid,CONVERT(-diaorusl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '分布式调入单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_distributedin 
									where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') and danjuzt = '已审核'
                                    and riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

                                    union all 
                                    select riqi,wuliaomc,diaoruck cangkumc,diaoruckid cangkuid,CONVERT(diaorusl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '分布式调入单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_distributedin 
									where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') and danjuzt = '已审核'
                                    and riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')


                                    union all
                                    select riqi,wuliaomc,diaochuck cangkumc,diaochuckid cangkuid,CONVERT(diaochusl,SIGNED) receiving,CONVERT(diaochusl,SIGNED) shipping,company,case when wuliaomc is not null then '分布式调出单' end `table` from erp_jd_dwd.erp_jd_dwd_dim_distributedout 
									where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签') and danjuzt = '已审核'
                                    and riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')


                                    union all
                                
                                    select 
                                    a.`riqi`,
                                    case when b.wuliaomc is null then a.wuliaomc else b.wuliaomc end wuliaomc,
                                    a.cangkumc,
                                    a.cangkuid,
                                    a.receiving,
                                    a.shipping,
                                    a.company,
                                    a.`table` 
                                    from (

                                    select tuiliaorq riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(-shituisl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '采购退料单' end `table` from erp_jd_ods.erp_jd_ods_dim_purchasereturn_wc_cwzx 
									where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
                                    and tuiliaorq >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')
                                    union all 
                                    select tuiliaorq riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(-shituisl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '采购退料单' end `table` from erp_jd_ods.erp_jd_ods_dim_purchasereturn_ms_cwzx 
									where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
                                    and tuiliaorq >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')
                                    union all 
                                    select tuiliaorq riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(-shituisl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '采购退料单' end `table` from erp_jd_ods.erp_jd_ods_dim_purchasereturn_yc_cwzx 
									where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
                                    and tuiliaorq >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')
                                    union all 
                                    select tuiliaorq riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(-shituisl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '采购退料单' end `table` from erp_jd_ods.erp_jd_ods_dim_purchasereturn_kyk_cwzx  
									where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
                                    and tuiliaorq >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

                                    union all
                                    select riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(shifasl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '采购入库单' end `table` from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_wc_cwzx 
									where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
                                    and riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')
                                    union all 
                                    select riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(shifasl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '采购入库单' end `table` from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_ms_cwzx 
									where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
                                    and riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')
                                    union all 
                                    select riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(shifasl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '采购入库单' end `table` from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_yc_cwzx 
									where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
                                    and riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')
                                    union all 
                                    select riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(shifasl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '采购入库单' end `table` from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_kyk_cwzx 
									where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')
                                    and riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')
                                    union all 
                                    select riqi,wuliaomc,wuliaobm,cangkumc,cangkuid,CONVERT(shifasl,SIGNED) receiving,case when wuliaomc is not null then 0 end shipping,company,case when wuliaomc is not null then '采购入库单' end `table` from erp_jd_ods.erp_jd_ods_dim_purchasereceiving_wc01_cwzx 
                                    where wuliaomc not in ('代收运费','测试物料1','管易云运费','激光标签-icon版','防伪贴','塑封膜','盲盒方形防伪标签','盲盒圆形防伪标签')    
                                    and riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')
                                    
                                    ) a
                                    LEFT JOIN(
                                    SELECT wuliaobm,wuliaomc FROM `erp_jd_dwd`.`erp_jd_dwd_fact_classify`
                                    ) b on a.wuliaobm = b.wuliaobm
                                    
                                    ;"""), engine.connect())
engine.dispose()


# df_warehouse   库存表
# ----------------------------------------------------------------------------------------------------- # 
df_warehouse['inventory'] = df_warehouse['receiving'] - df_warehouse['shipping']

df_warehouse.sort_values(['wuliaomc','riqi','receiving', 'shipping','company', 'table'],ascending=[True,True,False,False,False,False],inplace=True,ignore_index=True)
df_warehouse1 = df_warehouse.set_index(['riqi','wuliaomc']).groupby(['wuliaomc'])['inventory'].cumsum().reset_index().rename(columns = {'inventory':'inventory_wl'})
df_warehouse = pd.concat([df_warehouse,df_warehouse1['inventory_wl']],axis=1)

df_warehouse['refresh'] = datetime.now()


# *****************************************写入mysql*****************************************************#

insertsql(df_warehouse,'erp_jd_dws','erp_jd_dws_warehouse',
"INSERT INTO erp_jd_dws_warehouse(riqi,wuliaomc,cangkumc,cangkuid,receiving,shipping,company,`table`,inventory,inventory_wl,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
'riqi')