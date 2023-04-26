drop table if exists erp_jd_dwd.erp_jd_dwd_dim_acctagebalance;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_acctagebalance( 
    select * from erp_jd_ods.erp_jd_ods_dim_acctagebalance_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_acctagebalance_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_acctagebalance_xmgs
);


drop table if exists erp_jd_dwd.erp_jd_dwd_dim_allocation;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_allocation( 
    select * from erp_jd_ods.erp_jd_ods_dim_allocation_kyk_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_allocation_ms_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_allocation_wc_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_allocation_wc_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_allocation_yc_cwzx
);


drop table if exists erp_jd_dwd.erp_jd_dwd_dim_assemble;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_assemble( 
    select * from erp_jd_ods.erp_jd_ods_dim_assemble_kyk_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_assemble_wc_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_assemble_wc_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_assemble_yc_cwzx
);


drop table if exists erp_jd_dwd.erp_jd_dwd_dim_balance;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_balance( 
    select * from erp_jd_ods.erp_jd_ods_dim_balance_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_balance_xmgs
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_balance_dobest
);


drop table if exists erp_jd_dwd.erp_jd_dwd_dim_distributedin;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_distributedin( 
    select * from erp_jd_ods.erp_jd_ods_dim_distributedin_wc_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_distributedin_wc_dobest
);


drop table if exists erp_jd_dwd.erp_jd_dwd_dim_distributedout;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_distributedout( 
    select * from erp_jd_ods.erp_jd_ods_dim_distributedout_wc_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_distributedout_wc_dobest
);


drop table if exists erp_jd_dwd.erp_jd_dwd_dim_inventoryloss;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_inventoryloss( 
    select * from erp_jd_ods.erp_jd_ods_dim_inventoryloss_wc_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_inventoryloss_wc_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_inventoryloss_yc_cwzx
);


drop table if exists erp_jd_dwd.erp_jd_dwd_dim_inventoryprofit;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_inventoryprofit( 
    select * from erp_jd_ods.erp_jd_ods_dim_inventoryprofit_wc_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_inventoryprofit_wc_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_inventoryprofit_yc_cwzx
);


drop table if exists erp_jd_dwd.erp_jd_dwd_dim_othersreceiving;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_othersreceiving( 
    select *,year(riqi) `year`,month(riqi) `month` from erp_jd_ods.erp_jd_ods_dim_othersreceiving_kyk_cwzx
    union all 
    select *,year(riqi) `year`,month(riqi) `month` from erp_jd_ods.erp_jd_ods_dim_othersreceiving_wc_cwzx
    union all 
    select *,year(riqi) `year`,month(riqi) `month` from erp_jd_ods.erp_jd_ods_dim_othersreceiving_wc_dobest
    union all 
    select *,year(riqi) `year`,month(riqi) `month` from erp_jd_ods.erp_jd_ods_dim_othersreceiving_yc_cwzx
    union all 
    select *,year(riqi) `year`,month(riqi) `month` from erp_jd_ods.erp_jd_ods_dim_othersreceiving_yc_xmgs
);


drop table if exists erp_jd_dwd.erp_jd_dwd_dim_othersshipping;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_othersshipping( 
    select *,year(riqi) `year`,month(riqi) `month` from erp_jd_ods.erp_jd_ods_dim_othersshipping_kyk_cwzx
    where shenhezt = '已审核'
    union all 
    select *,year(riqi) `year`,month(riqi) `month` from erp_jd_ods.erp_jd_ods_dim_othersshipping_ms_cwzx
    where shenhezt = '已审核'
    union all 
    select *,year(riqi) `year`,month(riqi) `month` from erp_jd_ods.erp_jd_ods_dim_othersshipping_wc_cwzx
    where shenhezt = '已审核'
    union all 
    select *,year(riqi) `year`,month(riqi) `month` from erp_jd_ods.erp_jd_ods_dim_othersshipping_wc_dobest
    where shenhezt = '已审核'
    union all 
    select *,year(riqi) `year`,month(riqi) `month` from erp_jd_ods.erp_jd_ods_dim_othersshipping_yc_cwzx
    where shenhezt = '已审核'
    union all 
    select *,year(riqi) `year`,month(riqi) `month` from erp_jd_ods.erp_jd_ods_dim_othersshipping_yc_xmgs
    where shenhezt = '已审核'
);


drop table if exists erp_jd_dwd.erp_jd_dwd_dim_prepayment;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_prepayment( 
    select * from erp_jd_ods.erp_jd_ods_dim_prepayment_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_prepayment_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_prepayment_xmgs
);


drop table if exists erp_jd_dwd.erp_jd_dwd_dim_proceeds;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_proceeds( 
    select * from erp_jd_ods.erp_jd_ods_dim_proceeds_yc_xmgs
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_proceeds_yc_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_proceeds_wc_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_proceeds_kyk_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_proceeds_ms_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_proceeds_wc_cwzx
);


drop table if exists erp_jd_dwd.erp_jd_dwd_dim_voucher;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_voucher( 
    select * from erp_jd_ods.erp_jd_ods_dim_voucher_kyk_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_voucher_ms_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_voucher_ms_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_voucher_wc_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_voucher_wc_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_voucher_yc_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_voucher_yc_xmgs
);


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


drop table if exists erp_jd_dwd.erp_jd_dwd_dim_voucherentry;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_voucherentry( 
    select * from erp_jd_ods.erp_jd_ods_dim_voucherentry_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_voucherentry_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_voucherentry_xmgs
);


drop table if exists erp_jd_dwd.erp_jd_dwd_dim_voucherpayable;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_voucherpayable( 
    select *,case when caigouddh is not null then replace(caigouddh,'-1','') end caigouddh_1
    from erp_jd_ods.erp_jd_ods_dim_voucherpayable_kyk_cwzx
    union all 
    select *,case when caigouddh is not null then replace(caigouddh,'-1','') end caigouddh_1 
    from erp_jd_ods.erp_jd_ods_dim_voucherpayable_ms_cwzx
    union all 
    select *,case when caigouddh is not null then replace(caigouddh,'-1','') end caigouddh_1 
    from erp_jd_ods.erp_jd_ods_dim_voucherpayable_ms_dobest
    union all 
    select *,case when caigouddh is not null then replace(caigouddh,'-1','') end caigouddh_1 
    from erp_jd_ods.erp_jd_ods_dim_voucherpayable_wc_cwzx
    union all 
    select *,case when caigouddh is not null then replace(caigouddh,'-1','') end caigouddh_1 
    from erp_jd_ods.erp_jd_ods_dim_voucherpayable_wc_dobest
    union all 
    select *,case when caigouddh is not null then replace(caigouddh,'-1','') end caigouddh_1 
    from erp_jd_ods.erp_jd_ods_dim_voucherpayable_yc_cwzx
    union all 
    select *,case when caigouddh is not null then replace(caigouddh,'-1','') end caigouddh_1 
    from erp_jd_ods.erp_jd_ods_dim_voucherpayable_yc_xmgs
);


drop table if exists erp_jd_dwd.erp_jd_dwd_fact_account;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_fact_account( 
    select * from erp_jd_ods.erp_jd_ods_fact_account_kyk_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_account_ms_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_account_ms_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_account_wc_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_account_wc_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_account_yc_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_fact_account_yc_xmgs
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
