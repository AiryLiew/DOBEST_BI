drop table if exists erp_jd_dws.erp_jd_dws_acctagebalance;
CREATE TABLE erp_jd_dws.erp_jd_dws_acctagebalance( 
    SELECT distinct 
    a.`kehumc` ,
    a.`kehubm` ,
    a.`ffleX6` ,
    b.fname `账簿` ,
    c.`ffullname` ,
    c.`fnumber` ,
    c.`fname` ,
    x.* 
    FROM erp_jd_dwd.erp_jd_dwd_dim_acctagebalance x

    left join(
        SELECT fid,
        ffleX6, 
        `kehumc` ,
        `kehubm` 
        FROM erp_jd_dwd.erp_jd_dwd_fact_flexitemdetailv a
        left join(
            SELECT fmasterid ,  
            `kehumc` ,
            `kehubm` 
            FROM erp_jd_dwd.erp_jd_dwd_fact_customer
        ) b on b.fmasterid = a.ffleX6
    ) a on x.`fDetailID` = a.fid

    left join(
        SELECT fbookid ,fname
        FROM erp_jd_dwd.erp_jd_dwd_fact_accountbookl
    ) b on x.`faccountbookid` = b.`fbookid`

    left join(
        SELECT * 
        FROM erp_jd_dws.erp_jd_dws_fact_account
    ) c on x.faccountid = c.facctid
);