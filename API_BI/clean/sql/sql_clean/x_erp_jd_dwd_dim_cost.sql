drop table if exists erp_jd_dwd.erp_jd_dwd_dim_cost;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_cost( 
    SELECT wlmc_all wuliaomc,
    round(sum(jiashuihj)/sum(caigousl_new),2) cost
    FROM erp_jd_dwd.erp_jd_dwd_dim_purchaseorders 
    group by wlmc_all
    having sum(jiashuihj)/sum(caigousl_new)<>0

    union all

    SELECT a.wuliaomc,
    round(sum(a.zongchengb)/sum(a.shifasl)*1.13,2) cost 
    FROM erp_jd_dwd.erp_jd_dwd_dim_saleshipping a

    left join(
    SELECT distinct wlmc_all
    FROM erp_jd_dwd.erp_jd_dwd_dim_purchaseorders 
    group by wlmc_all
    having sum(jiashuihj)/sum(caigousl_new)<>0
    ) b on a.wuliaomc = b.wlmc_all
    where b.wlmc_all is null 
    group by a.wuliaomc
);