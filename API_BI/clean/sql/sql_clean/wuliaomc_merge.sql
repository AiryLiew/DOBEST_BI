drop TABLE IF EXISTS erp_jd_dws.wuliaomc_merge;
CREATE TABLE erp_jd_dws.wuliaomc_merge(
SELECT DISTINCT wuliaomc, wlmc_all
FROM erp_jd_dwd.erp_jd_dwd_dim_purchaseorders
);