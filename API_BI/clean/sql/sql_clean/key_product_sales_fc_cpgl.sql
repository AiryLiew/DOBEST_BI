drop table if exists erp_jd_ads.key_product_sales_fc_cpgl;
CREATE TABLE erp_jd_ads.key_product_sales_fc_cpgl(
SELECT `产品大类`,
       `产品中类`,
       `产品小类`,
       `产品名称`,
       `日期`,
       yearweek(日期,1) 年周,
       case when yearweek(日期,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 7 day),1) then '本周' 
       when yearweek(日期,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 14 day),1) then '上周'
       else '其他' end 分类,
       ifnull(`渠道仓销量`,0) `渠道仓销量`,
       ifnull(`渠道仓销售额`,0) `渠道仓销售额`,
       ifnull(`电商仓销量`,0) `电商仓销量`,
       ifnull(`电商仓销售额`,0) `电商仓销售额`,
       ifnull(`泳淳电商仓销量`,0) `泳淳电商仓销量`,
       ifnull(`泳淳电商仓销售额`,0) `泳淳电商仓销售额`,
       ifnull(`总销量`,0) `总销量`,
       ifnull(`总销售额`,0) `总销售额`,
       ifnull(`毛利`,0) `毛利`,
       ifnull(`赠品数量`,0) `赠品数量`
FROM erp_jd_ads.key_product_sales_fc
where `产品中类` not in ('海外系列','阵面对决','IP系列','自研B端剧本杀','其他','剧本杀配件','电商剧本杀道具','收藏卡') 
and `产品大类` in ('欢乐坊','推理桌游','三国杀','Yokakids','周边')
and `产品名称` not like '扑克三国杀%'
and `产品名称` not like '贵人鸟资源卡包第一弹%'


UNION all

SELECT `产品大类`,
       `产品中类`,
       `产品小类`,
       `产品名称`,
       `日期`,
       yearweek(日期,1) 年周,
       case when yearweek(日期,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 7 day),1) then '本周' 
       when yearweek(日期,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 14 day),1) then '上周'
       else '其他' end 分类,
       ifnull(`渠道仓销量`,0) `渠道仓销量`,
       ifnull(`渠道仓销售额`,0) `渠道仓销售额`,
       ifnull(`电商仓销量`,0) `电商仓销量`,
       ifnull(`电商仓销售额`,0) `电商仓销售额`,
       ifnull(`泳淳电商仓销量`,0) `泳淳电商仓销量`,
       ifnull(`泳淳电商仓销售额`,0) `泳淳电商仓销售额`,
       ifnull(`总销量`,0) `总销量`,
       ifnull(`总销售额`,0) `总销售额`,
       ifnull(`毛利`,0) `毛利`,
       ifnull(`赠品数量`,0) `赠品数量`
FROM erp_jd_ads.key_product_sales_fc
where `产品大类` = '周边'
and `产品小类` = '其他闪'


UNION all

SELECT `产品大类`,
       `产品中类`,
       `产品小类`,
       `产品名称`,
       `日期`,
       yearweek(日期,1) 年周,
       case when yearweek(日期,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 7 day),1) then '本周' 
       when yearweek(日期,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 14 day),1) then '上周'
       else '其他' end 分类,
       ifnull(`渠道仓销量`,0) `渠道仓销量`,
       ifnull(`渠道仓销售额`,0) `渠道仓销售额`,
       ifnull(`电商仓销量`,0) `电商仓销量`,
       ifnull(`电商仓销售额`,0) `电商仓销售额`,
       ifnull(`泳淳电商仓销量`,0) `泳淳电商仓销量`,
       ifnull(`泳淳电商仓销售额`,0) `泳淳电商仓销售额`,
       ifnull(`总销量`,0) `总销量`,
       ifnull(`总销售额`,0) `总销售额`,
       ifnull(`毛利`,0) `毛利`,
       ifnull(`赠品数量`,0) `赠品数量`
FROM erp_jd_ads.key_product_sales_fc
where `产品大类` in ( '三国杀','Yokakids')
and `产品中类` = 'IP系列'



UNION all

SELECT `产品大类`,
       `产品中类`,
       `产品小类`,
       `产品名称`,
       `日期`,
       yearweek(日期,1) 年周,
       case when yearweek(日期,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 7 day),1) then '本周' 
       when yearweek(日期,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 14 day),1) then '上周'
       else '其他' end 分类,
       ifnull(`渠道仓销量`,0) `渠道仓销量`,
       ifnull(`渠道仓销售额`,0) `渠道仓销售额`,
       ifnull(`电商仓销量`,0) `电商仓销量`,
       ifnull(`电商仓销售额`,0) `电商仓销售额`,
       ifnull(`泳淳电商仓销量`,0) `泳淳电商仓销量`,
       ifnull(`泳淳电商仓销售额`,0) `泳淳电商仓销售额`,
       ifnull(`总销量`,0) `总销量`,
       ifnull(`总销售额`,0) `总销售额`,
       ifnull(`毛利`,0) `毛利`,
       ifnull(`赠品数量`,0) `赠品数量`
FROM erp_jd_ads.key_product_sales_fc
where `产品名称`  like '扑克三国杀%'
and `总销售额`<>0
);