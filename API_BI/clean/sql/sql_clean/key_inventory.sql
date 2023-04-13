drop table if exists erp_jd_ads.key_inventory;
CREATE TABLE erp_jd_ads.key_inventory( 
    SELECT 
    x.wuliaomc `物料名称` ,
    a.`产品大类` ,
    a.`产品中类` ,
    a.`产品小类` ,
    x.`table` `单据类型` ,
    sum(x.receiving) `入库数量` ,
    sum(x.shipping) `出库数量` ,
    sum(x.inventory) `库存数量` 
    FROM erp_jd_dws.erp_jd_dws_warehouse x

    left join(
        select classify `产品大类`  ,
        classify_1 `产品中类`  ,
        classify_2 `产品小类`  ,
        wuliaomc `物料名称` 
        FROM erp_jd_dwd.erp_jd_dwd_fact_classify
    ) a on a.`物料名称` = x.wuliaomc

    group by x.wuliaomc,x.`table`
);
