drop table if exists www_bi_ads.wlgzs_hxzy_gsyj_zx_lastmonth;
CREATE TABLE www_bi_ads.wlgzs_hxzy_gsyj_zx_lastmonth ( 
select
a.classify_2 `产品小类` ,
a.`wuliaomc` `物料名称` ,
ifnull(sum(a.`12个月以上且库存数量>=100`),0)+ifnull(sum(a.`15个月以上`),0)  `滞销数量` ,
sum(a.`12个月以上且库存数量>=100`) `12个月以上且库存数量>=100` ,
sum(a.`15个月以上`)  `15个月以上` ,
b.`cost`*(ifnull(sum(a.`12个月以上且库存数量>=100`),0)+ifnull(sum(a.`15个月以上`),0) )  `滞销成本` 
from(
SELECT a.classify_2,
x.`wuliaomc`,
case when x.`doi`>=450 then x.surplus end `15个月以上`,
case when x.`doi`>=360 and x.`doi`<450 and x.`surplus`>=100 then  x.surplus end `12个月以上且库存数量>=100`
FROM erp_jd_dws.erp_jd_dws_doi_lastmonth x

left join(
SELECT classify ,classify_1 ,classify_2 ,wuliaomc 
FROM erp_jd_dwd.erp_jd_dwd_fact_classify 
) a on a.wuliaomc = x.wuliaomc
where a.classify='核心桌游'
and a.classify_1 <> '电商自采'
and a.classify_2 <> '配件'
) a

left join(
SELECT * 
FROM erp_jd_dwd.erp_jd_dwd_dim_cost 
) b on a.wuliaomc = b.wuliaomc

group by a.`wuliaomc`
having ifnull(sum(a.`12个月以上且库存数量>=100`),0)+ifnull(sum(a.`15个月以上`),0)>0
);