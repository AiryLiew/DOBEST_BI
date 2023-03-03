drop table if exists erp_jd_dws.erp_jd_dws_closebalance;
CREATE TABLE erp_jd_dws.erp_jd_dws_closebalance(   
SELECT distinct 
`账簿`,
`kehumc` `客户名称` ,
`kehubm` `客户编码` ,
`ffullname` `科目名称`,
`fnumber` `科目编码`,
year(`fbusdate`) `年` ,
month(`fbusdate`) `月`,
`fbusdate` `日期`,
`fbeginbalance` `借方金额`,
case when `kehumc` is not null then 0 end `贷方金额`,
`fbusdate` `修改日期`,
case when `kehumc` is not null then '期初' END`审核状态`
FROM erp_jd_dws.erp_jd_dws_acctagebalance
where `fbusdate` > '2019-12-31'

union all

SELECT  
`账簿` ,
`kehumc` `客户名称`,
`kehubm` `客户编码`,
`ffullname` `科目名称`,
`fnumber` `科目编码` ,
`fyear` `年`,
`fperiod` `月`,
`fdate` `日期`,
`fdebit` `借方金额`,
`fcredit` `贷方金额`,
`fmodifydate` `修改日期`,
case when `fdocumentstatus`='A' then '创建' 
when `fdocumentstatus`='B' then '审核中' 
when `fdocumentstatus`='C' then '已审核' 
when `fdocumentstatus`='D' then '重新审核' 
when `fdocumentstatus`='Z' then '暂存' END`审核状态`
FROM erp_jd_dws.erp_jd_dws_voucher_merge 
);