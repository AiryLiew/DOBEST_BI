drop table if exists erp_jd_dwd.erp_jd_dwd_dim_acctagebalance;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_acctagebalance( 
    select * from erp_jd_ods.erp_jd_ods_dim_acctagebalance_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_acctagebalance_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_acctagebalance_xmgs
);






drop table if exists erp_jd_dwd.erp_jd_dwd_dim_balance;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_balance( 
    select * from erp_jd_ods.erp_jd_ods_dim_balance_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_balance_xmgs
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_balance_dobest
);






drop table if exists erp_jd_dwd.erp_jd_dwd_dim_prepayment;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_prepayment( 
    select * from erp_jd_ods.erp_jd_ods_dim_prepayment_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_prepayment_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_prepayment_xmgs
);








drop table if exists erp_jd_dwd.erp_jd_dwd_dim_voucherentry;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_voucherentry( 
    select * from erp_jd_ods.erp_jd_ods_dim_voucherentry_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_voucherentry_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_voucherentry_xmgs
);









drop table if exists erp_jd_dwd.erp_jd_dwd_fact_accountbookl;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_fact_accountbookl( 
    select * from erp_jd_ods.erp_jd_ods_fact_accountbookl_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_accountbookl_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_accountbookl_xmgs
);







drop table if exists erp_jd_dwd.erp_jd_dwd_fact_accountl;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_fact_accountl( 
    select * from erp_jd_ods.erp_jd_ods_fact_accountl_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_accountl_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_accountl_xmgs
);







drop table if exists erp_jd_dwd.erp_jd_dwd_fact_assistantdataentry;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_fact_assistantdataentry( 
    select * from erp_jd_ods.erp_jd_ods_fact_assistantdataentry_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_assistantdataentry_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_assistantdataentry_xmgs
);







drop table if exists erp_jd_dwd.erp_jd_dwd_fact_customer;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_fact_customer( 
    select * from erp_jd_ods.erp_jd_ods_fact_client_kyk_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_client_ms_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_client_wc_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_client_wc01_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_client_yc_cwzx
);







drop table if exists erp_jd_dwd.erp_jd_dwd_fact_flexitemdetailv;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_fact_flexitemdetailv( 
    select * from erp_jd_ods.erp_jd_ods_fact_flexitemdetailv_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_flexitemdetailv_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_flexitemdetailv_xmgs
);







drop table if exists erp_jd_dwd.erp_jd_dwd_fact_flexitemproperty;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_fact_flexitemproperty( 
    select * from erp_jd_ods.erp_jd_ods_fact_flexitemproperty_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_flexitemproperty_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_flexitemproperty_xmgs
);







drop table if exists erp_jd_dwd.erp_jd_dwd_fact_lookupclass;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_fact_lookupclass( 
    select * from erp_jd_ods.erp_jd_ods_fact_lookupclass_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_lookupclass_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_lookupclass_xmgs
);







drop table if exists erp_jd_dwd.erp_jd_dwd_fact_vouchergroupl;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_fact_vouchergroupl( 
    select * from erp_jd_ods.erp_jd_ods_fact_vouchergroupl_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_vouchergroupl_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_vouchergroupl_xmgs
);
