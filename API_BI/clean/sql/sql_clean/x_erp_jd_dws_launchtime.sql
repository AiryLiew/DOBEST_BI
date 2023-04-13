drop table if exists erp_jd_dws.`erp_jd_dws_launchtime`;
CREATE TABLE erp_jd_dws.`erp_jd_dws_launchtime` (
    SELECT   `wlmc_all` ,
    min(riqi)  `riqi`,
    year(min(riqi))  `year` ,
    month(min(riqi))  `month`
    FROM erp_jd_dwd.erp_jd_dwd_dim_purchasereceiving 
    group by wlmc_all
);