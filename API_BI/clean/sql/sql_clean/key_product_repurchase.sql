drop table if exists erp_jd_ads.`key_product_repurchase`;
CREATE TABLE erp_jd_ads.`key_product_repurchase` (
SELECT 
a.`产品大类` ,
a.`产品中类` ,
a.`产品小类` ,
x.wuliaomc `物料名称`,
x.kehumc  `客户名称` ,
x.riqi `日期` ,
year(x.riqi)  `年` ,
month(x.riqi) `月` ,
count(distinct (x.danjubh)) `下单次数` ,
sum(x.xiaoshousl) `订单量` ,
sum(x.jiashuihj) `订单收入` 
FROM erp_jd_dwd.erp_jd_dwd_dim_saleorders x
 
left join(
select classify `产品大类`  ,
classify_1 `产品中类`  ,
classify_2 `产品小类`  ,
wuliaomc `物料名称` 
FROM erp_jd_dwd.erp_jd_dwd_fact_classify
) a on a.`物料名称` = x.wuliaomc

group by x.wuliaomc,
x.kehumc,
x.riqi
);