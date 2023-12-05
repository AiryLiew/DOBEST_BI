drop table if exists erp_jd_dws.erp_jd_dws_fact_account;
CREATE TABLE erp_jd_dws.erp_jd_dws_fact_account(   
    SELECT a.`facctid` ,
      a.`fnumber` ,
      b.`fname` ,
      b.`ffullname`
    FROM erp_jd_dwd.erp_jd_dwd_fact_account a

    left join(
        SELECT `facctid`,  
        `fname` ,
        `ffullname`
        FROM erp_jd_dwd.erp_jd_dwd_fact_accountl 
    ) b on a.`facctid` = b.`facctid`
);