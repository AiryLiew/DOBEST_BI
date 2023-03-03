drop table if exists www_bi_ads.wlgzs_hxzy_gsyj_zx;
CREATE TABLE www_bi_ads.wlgzs_hxzy_gsyj_zx ( 
select
a.classify_2 `产品小类` ,
a.`wuliaomc` `物料名称` ,
ifnull(sum(a.`12个月以上且库存数量>=100`),0)+ifnull(sum(a.`15个月以上`),0)  `滞销数量` ,
sum(a.`12个月以上且库存数量>=100`) `12个月以上且库存数量>=100` ,
sum(a.`15个月以上`)  `15个月以上` ,
b.`上市日期` ,
b.`采购成本` ,
b.`采购成本`*(ifnull(sum(a.`12个月以上且库存数量>=100`),0)+ifnull(sum(a.`15个月以上`),0) )  `滞销成本` ,
b.`动销率` ,
b.`毛利率` ,
b.`库存结余` ,
b.`工厂结余` ,
b.`库存结余+工厂结余` ,
b.`月均销量`
from(
SELECT a.classify_2,
x.`wuliaomc`,
case when x.`doi`>=450 then x.surplus end `15个月以上`,
case when x.`doi`>=360 and x.`doi`<450 and x.`surplus`>=100 then  x.surplus end `12个月以上且库存数量>=100`
FROM erp_jd_dws.erp_jd_dws_doi x

left join(
SELECT classify ,classify_1 ,classify_2 ,wuliaomc 
FROM erp_jd_dwd.erp_jd_dwd_fact_classify 
) a on a.wuliaomc = x.wuliaomc
where a.classify='核心桌游'
and a.classify_1 <> '电商自采'
and a.classify_2 <> '配件'
) a

left join(
SELECT  
`物料名称`,
`上市日期` ,
  `采购成本` ,
  `动销率` ,
  `毛利率` ,
  `库存结余（不含残次仓）` 库存结余,
  `工厂结余` ,
ifnull(`库存结余（不含残次仓）`,0)+ifnull(`工厂结余`,0)  `库存结余+工厂结余` ,
  `月均销量`
FROM erp_jd_ads.key_product 
) b on a.wuliaomc = b.`物料名称`

group by a.`wuliaomc`
having ifnull(sum(a.`12个月以上且库存数量>=100`),0)+ifnull(sum(a.`15个月以上`),0)>0
and b.`库存结余+工厂结余`>0
);