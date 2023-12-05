drop table if exists erp_jd_ads.key_warehouse_diff;
CREATE TABLE erp_jd_ads.key_warehouse_diff( 
    SELECT 
    a.`产品大类`  ,
    a.`产品中类`  ,
    a.`产品小类`  ,
    x.`wuliaomc` 物料名称,
    x.`cangkumc` `仓库名称`,
    ifnull(sum(case when x.riqi<'2022-01-01' then x.`inventory` end),0) `2021年底库存结余`,  
    ifnull(sum(case when x.riqi<'2023-01-01' then x.`inventory` end),0) `2022年底库存结余`,
    ifnull(sum(x.`inventory`),0) `库存结余`,
    ifnull(sum(case when x.riqi<'2022-01-01' then x.`inventory` end),0)-ifnull(sum(case when x.riqi<'2023-01-01' then x.`inventory` end),0) `上年实际库存减少`,
    ifnull(sum(case when x.riqi<'2023-01-01' then x.`inventory` end),0)-ifnull(sum(x.`inventory`),0) `本年实际库存减少`
    FROM erp_jd_dws.erp_jd_dws_warehouse x

    left join(
        select classify `产品大类`  ,
        classify_1 `产品中类`  ,
        classify_2 `产品小类`  ,
        wuliaomc `物料名称` 
        FROM erp_jd_dwd.erp_jd_dwd_fact_classify
    ) a on a.`物料名称` = x.wuliaomc

    group by x.`wuliaomc` ,
    x.`cangkumc`
    having ifnull(sum(case when x.riqi<'2022-01-01' then x.`inventory` end),0)+ ifnull(sum(case when x.riqi<'2023-01-01' then x.`inventory` end),0)+ifnull(sum(x.`inventory`),0)<>0
);