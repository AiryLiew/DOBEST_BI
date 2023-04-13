drop table if exists erp_jd_ads.erp_jd_ads_balance_1122_06;
CREATE TABLE erp_jd_ads.erp_jd_ads_balance_1122_06( 
    SELECT distinct 
    `kehumc` `客户名称` ,
    `kehubm` `客户编码`,
    `账簿` ,
    `ffullname` `科目名称`,
    `fnumber` `科目编码`,
    `fBeginBalance` `期初余额借方`,
    `fDebit` `本期发生借方`,
    `fCredit` `本期发生贷方`,
    `fYtdDebit` `本年累计借方`,
    `fYtdCredit` `本年累计贷方`,
    `fEndBalance` `期末余额借方`,
    `fYearPeriod` `年月`
    FROM erp_jd_dws.erp_jd_dws_balance
    where `fnumber` = '1122.06'
);