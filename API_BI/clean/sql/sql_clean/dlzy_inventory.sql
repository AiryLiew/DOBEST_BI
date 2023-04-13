drop table if exists www_bi_ads.dlzy_inventory;
CREATE TABLE www_bi_ads.dlzy_inventory( 
  SELECT `riqi`,
  `wuliaomc` ,
  xiaoshousl `receiving` ,
  kehumc `cangkumc` ,
  case when `wuliaomc` is not null then 0 end `shipping` ,
  case when `wuliaomc` is not null then '发货' end `类型` ,
  case when `wuliaomc` is not null then '自研' end `属性`,
  xiaoshousl `inventory` 
  FROM erp_jd_dws.qd_saleorders_zy 

  union all

  SELECT `riqi`,
  `wuliaomc` ,
  xiaoshousl `receiving` ,
  kehumc `cangkumc` ,
  case when `wuliaomc` is not null then 0 end `shipping` ,
  case when `wuliaomc` is not null then '发货' end `类型` ,
  case when `wuliaomc` is not null then '代理' end `属性`,
  xiaoshousl `inventory` 
  FROM erp_jd_dws.qd_saleorders_dl 

  union all

  SELECT `riqi`,
  `wuliaomc` ,
  case when `wuliaomc` is not null then 0 end `receiving` ,
  kehumc `cangkumc` ,
  shifasl `shipping` ,
  case when `wuliaomc` is not null then '销售' end `类型` ,
  case when `wuliaomc` is not null then '自研' end `属性`,
  -shifasl `inventory` 
  FROM erp_jd_dws.qd_saleshipping_zy 

  union all

  SELECT `riqi`,
  `wuliaomc` ,
  case when `wuliaomc` is not null then 0 end `receiving` ,
  kehumc `cangkumc` ,
  shifasl `shipping` ,
  case when `wuliaomc` is not null then '销售' end `类型` ,
  case when `wuliaomc` is not null then '代理' end `属性`,
  -shifasl `inventory` 
  FROM erp_jd_dws.qd_saleshipping_dl 

  union all

  SELECT `riqi`,
  `wuliaomc` ,
  case when `wuliaomc` is not null then 0 end `receiving` ,
  kehumc `cangkumc` ,
  shifasl `shipping` ,
  case when `wuliaomc` is not null then '退货' end `类型` ,
  case when `wuliaomc` is not null then '自研' end `属性`,
  -shifasl `inventory` 
  FROM erp_jd_dws.qd_salereturn_zy 

  union all

  SELECT `riqi`,
  `wuliaomc` ,
  case when `wuliaomc` is not null then 0 end `receiving` ,
  case when guanlianxskh is null then diaochubgzmc end `cangkumc` ,
  diaobosl `shipping` ,
  case when `wuliaomc` is not null then '退货' end `类型` ,
  `属性`,
  -diaobosl `inventory` 
  FROM erp_jd_dws.qd_allocation_zy 
);