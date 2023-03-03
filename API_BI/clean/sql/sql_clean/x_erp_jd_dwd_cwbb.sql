drop table if exists erp_jd_dwd.erp_jd_dwd_dim_voucher_cwzx;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_voucher_cwzx( 
select * from erp_jd_ods.erp_jd_ods_dim_voucher_kyk_cwzx
union all 
select * from erp_jd_ods.erp_jd_ods_dim_voucher_ms_cwzx
union all 
select * from erp_jd_ods.erp_jd_ods_dim_voucher_wc_cwzx
union all 
select * from erp_jd_ods.erp_jd_ods_dim_voucher_yc_cwzx
);

