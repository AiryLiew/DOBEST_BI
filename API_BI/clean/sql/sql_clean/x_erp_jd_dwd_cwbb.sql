delete FROM erp_jd_dwd.erp_jd_dwd_dim_voucher_cwzx
where fdate>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01');

INSERT INTO erp_jd_dwd.erp_jd_dwd_dim_voucher_cwzx( `fVoucherID`,`faccountbookid` ,`facctorgid` ,`fdate` ,`fyear` ,
`fperiod` ,`fbillno` ,`fvouchergroupid` ,`fvouchergroupno` ,`fattachments`, `freference` ,`fsettletypeid` ,
`fsettleno` ,`fbasecurrencyid` ,`fdebittotal`,`fcredittotal`,`fcreatedate` ,`fmodifydate` ,
`fdocumentstatus` ,`fchecked` ,`fcheckerid` ,`fauditdate` ,`fposted` ,`fposterid` ,`fpostdate` ,
`fAdjustPeriod` ,`fInvalid` ,`fmapvchid` ,`fSourceBillKey` ,`fisadjustvoucher` ,`company` ,`refresh_jk` 
)
select `fVoucherID`,`faccountbookid` ,`facctorgid` ,`fdate` ,`fyear` ,
`fperiod` ,`fbillno` ,`fvouchergroupid` ,`fvouchergroupno` ,`fattachments`, `freference` ,`fsettletypeid` ,
`fsettleno` ,`fbasecurrencyid` ,`fdebittotal`,`fcredittotal`,`fcreatedate` ,`fmodifydate` ,
`fdocumentstatus` ,`fchecked` ,`fcheckerid` ,`fauditdate` ,`fposted` ,`fposterid` ,`fpostdate` ,
`fAdjustPeriod` ,`fInvalid` ,`fmapvchid` ,`fSourceBillKey` ,`fisadjustvoucher` ,`company` ,`refresh_jk` 
from erp_jd_ods.erp_jd_ods_dim_voucher_kyk_cwzx
where fdate>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

union all 
select `fVoucherID`,`faccountbookid` ,`facctorgid` ,`fdate` ,`fyear` ,
`fperiod` ,`fbillno` ,`fvouchergroupid` ,`fvouchergroupno` ,`fattachments`, `freference` ,`fsettletypeid` ,
`fsettleno` ,`fbasecurrencyid` ,`fdebittotal`,`fcredittotal`,`fcreatedate` ,`fmodifydate` ,
`fdocumentstatus` ,`fchecked` ,`fcheckerid` ,`fauditdate` ,`fposted` ,`fposterid` ,`fpostdate` ,
`fAdjustPeriod` ,`fInvalid` ,`fmapvchid` ,`fSourceBillKey` ,`fisadjustvoucher` ,`company` ,`refresh_jk` 
from erp_jd_ods.erp_jd_ods_dim_voucher_ms_cwzx
where fdate>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

union all 
select `fVoucherID`,`faccountbookid` ,`facctorgid` ,`fdate` ,`fyear` ,
`fperiod` ,`fbillno` ,`fvouchergroupid` ,`fvouchergroupno` ,`fattachments`, `freference` ,`fsettletypeid` ,
`fsettleno` ,`fbasecurrencyid` ,`fdebittotal`,`fcredittotal`,`fcreatedate` ,`fmodifydate` ,
`fdocumentstatus` ,`fchecked` ,`fcheckerid` ,`fauditdate` ,`fposted` ,`fposterid` ,`fpostdate` ,
`fAdjustPeriod` ,`fInvalid` ,`fmapvchid` ,`fSourceBillKey` ,`fisadjustvoucher` ,`company` ,`refresh_jk` 
from erp_jd_ods.erp_jd_ods_dim_voucher_wc_cwzx
where fdate>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

union all 
select `fVoucherID`,`faccountbookid` ,`facctorgid` ,`fdate` ,`fyear` ,
`fperiod` ,`fbillno` ,`fvouchergroupid` ,`fvouchergroupno` ,`fattachments`, `freference` ,`fsettletypeid` ,
`fsettleno` ,`fbasecurrencyid` ,`fdebittotal`,`fcredittotal`,`fcreatedate` ,`fmodifydate` ,
`fdocumentstatus` ,`fchecked` ,`fcheckerid` ,`fauditdate` ,`fposted` ,`fposterid` ,`fpostdate` ,
`fAdjustPeriod` ,`fInvalid` ,`fmapvchid` ,`fSourceBillKey` ,`fisadjustvoucher` ,`company` ,`refresh_jk` 
from erp_jd_ods.erp_jd_ods_dim_voucher_wc01_cwzx
where fdate>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

union all 
select `fVoucherID`,`faccountbookid` ,`facctorgid` ,`fdate` ,`fyear` ,
`fperiod` ,`fbillno` ,`fvouchergroupid` ,`fvouchergroupno` ,`fattachments`, `freference` ,`fsettletypeid` ,
`fsettleno` ,`fbasecurrencyid` ,`fdebittotal`,`fcredittotal`,`fcreatedate` ,`fmodifydate` ,
`fdocumentstatus` ,`fchecked` ,`fcheckerid` ,`fauditdate` ,`fposted` ,`fposterid` ,`fpostdate` ,
`fAdjustPeriod` ,`fInvalid` ,`fmapvchid` ,`fSourceBillKey` ,`fisadjustvoucher` ,`company` ,`refresh_jk` 
from erp_jd_ods.erp_jd_ods_dim_voucher_yc_cwzx
where fdate>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')
;

