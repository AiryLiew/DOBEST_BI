drop table if exists erp_jd_ads.erp_jd_ads_addorders;
CREATE TABLE erp_jd_ads.erp_jd_ads_addorders(
    SELECT b.`产品大类` ,
    b.`产品中类` ,
    b.`产品小类` ,
    a.wlmc_all `产品名称`,
    c.riqi `上市时间` ,
    a.`采购日期` ,
    a.采购数量,
    a.`入库数量` ,
    case when a.rank_num=1 then '新品' else '加单' end `是否加单`,
    case when a.`未入库数量`= 0 then '已入库' when a.`入库数量`=0 then '未入库' else '部分入库' end `入库状态`
    from(
        SELECT wlmc_all,
        riqi `采购日期` ,
        rank() over(partition by wlmc_all order by riqi) rank_num,
        sum(caigousl_new) 采购数量,
        sum(leijirksl_new) `入库数量` ,
        sum(shengyurksl_new) `未入库数量` 
        FROM erp_jd_dwd.erp_jd_dwd_dim_purchaseorders
        group by wlmc_all,riqi
        having sum(caigousl_new)>0
    ) a
    
    left join(
        select classify `产品大类`  ,
        classify_1 `产品中类`  ,
        classify_2 `产品小类`  ,
        wuliaomc `物料名称` 
        FROM erp_jd_dwd.erp_jd_dwd_fact_classify
    ) b on b.`物料名称` = a.wlmc_all

    left join(
        SELECT wlmc_all,riqi 
        FROM erp_jd_dws.erp_jd_dws_launchtime
    ) c on c.wlmc_all = a.wlmc_all
);