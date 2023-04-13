drop table if exists erp_jd_dws.erp_jd_dws_voucher_merge;
CREATE TABLE erp_jd_dws.erp_jd_dws_voucher_merge( 
    SELECT distinct 
    d.`kehumc` ,
    d.`kehubm` ,
    d.`ffleX6` ,
    b.`facctid`,
    b.`fname` ,
    b.`ffullname` ,
    b.`fnumber` ,
    c.`fdebit`,
    c.`fcredit`,
    c.`fentryseq` ,
    c.`fdc` ,
    c.`famountfor`,
    c.`famount`,
    c.`fDetailID` ,
    c.`faccountid` ,
    c.`fexplanation` ,
    c.`fentryid`,
    a.`凭证字` ,
    a.`账簿` ,
    a.`fbookid`,
    a.`fVoucherID`,
    a.`facctorgid` ,
    a.`fdate` ,
    a.`fyear` ,
    a.`fperiod` ,
    a.`fbillno` ,
    a.`fvouchergroupid` ,
    a.`fvouchergroupno` ,
    a.`fdebittotal` ,
    a.`fcredittotal` ,
    a.`fcreatedate` ,
    a.`fmodifydate` ,
    a.`fdocumentstatus` 
    FROM erp_jd_ods.erp_jd_ods_dim_voucherentry_cwzx c


    left join(
        SELECT 
        c.`fname` `凭证字`,
        b.fname `账簿` ,
        b.`fbookid`,
        a.`fVoucherID`,
        a.`facctorgid` ,
        a.`fdate` ,
        a.`fyear` ,
        a.`fperiod` ,
        a.`fbillno` ,
        a.`fvouchergroupid` ,
        a.`fvouchergroupno` ,
        a.`fdebittotal` ,
        a.`fcredittotal` ,
        a.`fcreatedate` ,
        a.`fmodifydate` ,
        a.`fdocumentstatus` 
        FROM erp_jd_dwd.erp_jd_dwd_dim_voucher_cwzx a 

        left join(
            SELECT fbookid ,fname
            FROM erp_jd_dwd.erp_jd_dwd_fact_accountbookl
        ) b on a.`faccountbookid` = b.`fbookid`

        left join(
            SELECT `fvchgroupid`,
              `fname`
            FROM erp_jd_dwd.erp_jd_dwd_fact_vouchergroupl 
        ) c on a.`fvouchergroupid` = c.`fvchgroupid`

    ) a on a.`fVoucherID` = c.`fVoucherID`


    left join(
        SELECT * 
        FROM erp_jd_dws.erp_jd_dws_fact_account
    ) b on c.faccountid = b.facctid


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
    ) d on c.`fDetailID` = d.fid
);