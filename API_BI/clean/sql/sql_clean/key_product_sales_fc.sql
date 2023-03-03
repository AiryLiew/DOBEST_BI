drop table if exists erp_jd_ads.key_product_sales_fc;
CREATE TABLE erp_jd_ads.key_product_sales_fc(
SELECT b.`产品大类` ,
  b.`产品中类` ,
  b.`产品小类` ,
  a.wuliaomc `产品名称`,
  year(a.riqi) `年` ,
  quarter(a.riqi) `季度` ,
  month(a.riqi) `月` ,
  week(a.riqi) `周` ,
  day(a.riqi) `日` ,
  weekday(a.riqi) `星期` ,
  month(a.riqi)*100+day(a.riqi)`月_日` ,
  a.riqi `日期` ,
  sum(case when cangkumc like '%渠道%' then a.shifasl end) `渠道仓销量` ,
  sum(case when cangkumc like '%渠道%' then a.jiashuihj end) `渠道仓销售额` ,
  sum(case when cangkumc like '%-电商%' then a.shifasl end) `电商仓销量` ,
  sum(case when cangkumc like '%-电商%' then a.jiashuihj end) `电商仓销售额` ,
  sum(case when cangkumc like '%泳淳%' then a.shifasl end) `泳淳电商仓销量` ,
  sum(case when cangkumc like '%泳淳%' then a.jiashuihj end) `泳淳电商仓销售额` ,
  sum(a.shifasl) `总销量` ,
  sum(a.jiashuihj) `总销售额` ,
  sum(a.profit) `毛利` 
FROM erp_jd_dwd.erp_jd_dwd_dim_saleshipping a

left join(
select classify `产品大类`  ,
classify_1 `产品中类`  ,
classify_2 `产品小类`  ,
wuliaomc `物料名称` 
FROM erp_jd_dwd.erp_jd_dwd_fact_classify
) b on b.`物料名称` = a.wuliaomc

where a.kehumc not in ('杭州泳淳网络技术有限公司','杭州游卡文化创意有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司')
group by a.wuliaomc ,a.riqi
);