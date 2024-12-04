
delete FROM erp_jd_dwd.erp_jd_dwd_dim_proceeds
where shujuzx = '财务数据中心';

INSERT INTO erp_jd_dwd.erp_jd_dwd_dim_proceeds(`fid` ,`riqi` ,`danjulxmc` ,`danjulxdm` ,`danjubh` ,`bizhongid` ,`bizhongmc` ,
`yingshouje` ,`shishouje` ,`wanglaidwid` ,`wanglaidwmc` ,`shoukuanytbm` ,`shoukuanytmc` ,`company` ,`shujuzx` ,`refresh_jk`
)    
select `fid` ,`riqi` ,`danjulxmc` ,`danjulxdm` ,`danjubh` ,`bizhongid` ,`bizhongmc` ,
`yingshouje` ,`shishouje` ,`wanglaidwid` ,`wanglaidwmc` ,`shoukuanytbm` ,`shoukuanytmc` ,`company` ,`shujuzx` ,`refresh_jk` from erp_jd_ods.erp_jd_ods_dim_proceeds_yc_cwzx

union all 
select `fid` ,`riqi` ,`danjulxmc` ,`danjulxdm` ,`danjubh` ,`bizhongid` ,`bizhongmc` ,
`yingshouje` ,`shishouje` ,`wanglaidwid` ,`wanglaidwmc` ,`shoukuanytbm` ,`shoukuanytmc` ,`company` ,`shujuzx` ,`refresh_jk` from erp_jd_ods.erp_jd_ods_dim_proceeds_kyk_cwzx

union all 
select `fid` ,`riqi` ,`danjulxmc` ,`danjulxdm` ,`danjubh` ,`bizhongid` ,`bizhongmc` ,
`yingshouje` ,`shishouje` ,`wanglaidwid` ,`wanglaidwmc` ,`shoukuanytbm` ,`shoukuanytmc` ,`company` ,`shujuzx` ,`refresh_jk` from erp_jd_ods.erp_jd_ods_dim_proceeds_ms_cwzx

union all 
select `fid` ,`riqi` ,`danjulxmc` ,`danjulxdm` ,`danjubh` ,`bizhongid` ,`bizhongmc` ,
`yingshouje` ,`shishouje` ,`wanglaidwid` ,`wanglaidwmc` ,`shoukuanytbm` ,`shoukuanytmc` ,`company` ,`shujuzx` ,`refresh_jk` from erp_jd_ods.erp_jd_ods_dim_proceeds_wc_cwzx

union all 
select `fid` ,`riqi` ,`danjulxmc` ,`danjulxdm` ,`danjubh` ,`bizhongid` ,`bizhongmc` ,
`yingshouje` ,`shishouje` ,`wanglaidwid` ,`wanglaidwmc` ,`shoukuanytbm` ,`shoukuanytmc` ,`company` ,`shujuzx` ,`refresh_jk` from erp_jd_ods.erp_jd_ods_dim_proceeds_wc01_cwzx ;
   







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








delete FROM erp_jd_dwd.erp_jd_dwd_dim_voucherpayable
where shujuzx = '财务数据中心';

INSERT INTO erp_jd_dwd.erp_jd_dwd_dim_voucherpayable( `fid`,`riqi` ,`gongyingsid` ,`gongyingsmc` ,`danjubh` ,`wuliaobm` ,
  `wuliaomc` ,`danjia` ,`jijiasl` ,`hanshuidj` ,`shuilv` ,`jiashuihj` ,`feiyongcdbmdm` ,
  `feiyongcdbmmc` ,`yijiesje` ,`weijiesje` ,`weikaiphxje` ,`weikaiphxsl` ,`caigoubmdm` ,
  `caigoubmmc` ,`yuandanbh` ,`caigouddh` ,`danjuzt` ,`company` ,`shujuzx` ,`refresh_jk`,`caigouddh_1`  
)    
select `fid`,`riqi` ,`gongyingsid` ,`gongyingsmc` ,`danjubh` ,`wuliaobm` ,
`wuliaomc` ,`danjia` ,`jijiasl` ,`hanshuidj` ,`shuilv` ,`jiashuihj` ,`feiyongcdbmdm` ,
`feiyongcdbmmc` ,`yijiesje` ,`weijiesje` ,`weikaiphxje` ,`weikaiphxsl` ,`caigoubmdm` ,
`caigoubmmc` ,`yuandanbh` ,`caigouddh` ,`danjuzt` ,`company` ,`shujuzx` ,`refresh_jk`,case when caigouddh is not null then replace(caigouddh,'-1','') end caigouddh_1
from erp_jd_ods.erp_jd_ods_dim_voucherpayable_kyk_cwzx

union all 
select `fid`,`riqi` ,`gongyingsid` ,`gongyingsmc` ,`danjubh` ,`wuliaobm` ,
`wuliaomc` ,`danjia` ,`jijiasl` ,`hanshuidj` ,`shuilv` ,`jiashuihj` ,`feiyongcdbmdm` ,
`feiyongcdbmmc` ,`yijiesje` ,`weijiesje` ,`weikaiphxje` ,`weikaiphxsl` ,`caigoubmdm` ,
`caigoubmmc` ,`yuandanbh` ,`caigouddh` ,`danjuzt` ,`company` ,`shujuzx` ,`refresh_jk`,case when caigouddh is not null then replace(caigouddh,'-1','') end caigouddh_1 
from erp_jd_ods.erp_jd_ods_dim_voucherpayable_ms_cwzx

union all 
select `fid`,`riqi` ,`gongyingsid` ,`gongyingsmc` ,`danjubh` ,`wuliaobm` ,
`wuliaomc` ,`danjia` ,`jijiasl` ,`hanshuidj` ,`shuilv` ,`jiashuihj` ,`feiyongcdbmdm` ,
`feiyongcdbmmc` ,`yijiesje` ,`weijiesje` ,`weikaiphxje` ,`weikaiphxsl` ,`caigoubmdm` ,
`caigoubmmc` ,`yuandanbh` ,`caigouddh` ,`danjuzt` ,`company` ,`shujuzx` ,`refresh_jk`,case when caigouddh is not null then replace(caigouddh,'-1','') end caigouddh_1 
from erp_jd_ods.erp_jd_ods_dim_voucherpayable_wc_cwzx

union all 
select `fid`,`riqi` ,`gongyingsid` ,`gongyingsmc` ,`danjubh` ,`wuliaobm` ,
`wuliaomc` ,`danjia` ,`jijiasl` ,`hanshuidj` ,`shuilv` ,`jiashuihj` ,`feiyongcdbmdm` ,
`feiyongcdbmmc` ,`yijiesje` ,`weijiesje` ,`weikaiphxje` ,`weikaiphxsl` ,`caigoubmdm` ,
`caigoubmmc` ,`yuandanbh` ,`caigouddh` ,`danjuzt` ,`company` ,`shujuzx` ,`refresh_jk`,case when caigouddh is not null then replace(caigouddh,'-1','') end caigouddh_1 
from erp_jd_ods.erp_jd_ods_dim_voucherpayable_yc_cwzx

union all 
select `fid`,`riqi` ,`gongyingsid` ,`gongyingsmc` ,`danjubh` ,`wuliaobm` ,
`wuliaomc` ,`danjia` ,`jijiasl` ,`hanshuidj` ,`shuilv` ,`jiashuihj` ,`feiyongcdbmdm` ,
`feiyongcdbmmc` ,`yijiesje` ,`weijiesje` ,`weikaiphxje` ,`weikaiphxsl` ,`caigoubmdm` ,
`caigoubmmc` ,`yuandanbh` ,`caigouddh` ,`danjuzt` ,`company` ,`shujuzx` ,`refresh_jk`,case when caigouddh is not null then replace(caigouddh,'-1','') end caigouddh_1 
from erp_jd_ods.erp_jd_ods_dim_voucherpayable_wc01_cwzx;









delete FROM erp_jd_dwd.erp_jd_dwd_fact_account
where shujuzx = '财务数据中心';

INSERT INTO erp_jd_dwd.erp_jd_dwd_fact_account( `facctid` ,
  `fnumber` ,`fparentid` ,`fhelpercode` ,`fgroupid` ,`fdc` ,`faccttblid` ,`fiscash` ,`fisbank` ,
  `fisallocate` ,`fitemdetailid` ,`fisquantities` ,`funitgroupid` ,`funitid` ,`fisdetail` ,`flevel` ,
  `fcreateorgid` ,`fuseorgid` ,`fforbidstatus` ,`fmasterid` ,`fissyspreset` ,`fdocumentstatus` ,
  `fcfitemid` ,`focfitemid` ,`fcfindirectitemid` ,`focfindirectitemid` ,`fallcurrency` ,`fcurrencylist` ,
  `fcurrencys` ,`fisshowjournal` ,`famountdc` ,`fiscontact` ,`company` ,`shujuzx` ,`refresh_jk` )    
    select `facctid` ,`fnumber` ,`fparentid` ,`fhelpercode` ,`fgroupid` ,`fdc` ,`faccttblid` ,`fiscash` ,`fisbank` ,
  `fisallocate` ,`fitemdetailid` ,`fisquantities` ,`funitgroupid` ,`funitid` ,`fisdetail` ,`flevel` ,
  `fcreateorgid` ,`fuseorgid` ,`fforbidstatus` ,`fmasterid` ,`fissyspreset` ,`fdocumentstatus` ,
  `fcfitemid` ,`focfitemid` ,`fcfindirectitemid` ,`focfindirectitemid` ,`fallcurrency` ,`fcurrencylist` ,
  `fcurrencys` ,`fisshowjournal` ,`famountdc` ,`fiscontact` ,`company` ,`shujuzx` ,`refresh_jk` from erp_jd_ods.erp_jd_ods_fact_account_kyk_cwzx
    union all 
    select `facctid` ,`fnumber` ,`fparentid` ,`fhelpercode` ,`fgroupid` ,`fdc` ,`faccttblid` ,`fiscash` ,`fisbank` ,
  `fisallocate` ,`fitemdetailid` ,`fisquantities` ,`funitgroupid` ,`funitid` ,`fisdetail` ,`flevel` ,
  `fcreateorgid` ,`fuseorgid` ,`fforbidstatus` ,`fmasterid` ,`fissyspreset` ,`fdocumentstatus` ,
  `fcfitemid` ,`focfitemid` ,`fcfindirectitemid` ,`focfindirectitemid` ,`fallcurrency` ,`fcurrencylist` ,
  `fcurrencys` ,`fisshowjournal` ,`famountdc` ,`fiscontact` ,`company` ,`shujuzx` ,`refresh_jk` from erp_jd_ods.erp_jd_ods_fact_account_ms_cwzx
    union all 
    select `facctid` ,`fnumber` ,`fparentid` ,`fhelpercode` ,`fgroupid` ,`fdc` ,`faccttblid` ,`fiscash` ,`fisbank` ,
  `fisallocate` ,`fitemdetailid` ,`fisquantities` ,`funitgroupid` ,`funitid` ,`fisdetail` ,`flevel` ,
  `fcreateorgid` ,`fuseorgid` ,`fforbidstatus` ,`fmasterid` ,`fissyspreset` ,`fdocumentstatus` ,
  `fcfitemid` ,`focfitemid` ,`fcfindirectitemid` ,`focfindirectitemid` ,`fallcurrency` ,`fcurrencylist` ,
  `fcurrencys` ,`fisshowjournal` ,`famountdc` ,`fiscontact` ,`company` ,`shujuzx` ,`refresh_jk` from erp_jd_ods.erp_jd_ods_fact_account_wc_cwzx
    union all 
    select `facctid` ,`fnumber` ,`fparentid` ,`fhelpercode` ,`fgroupid` ,`fdc` ,`faccttblid` ,`fiscash` ,`fisbank` ,
  `fisallocate` ,`fitemdetailid` ,`fisquantities` ,`funitgroupid` ,`funitid` ,`fisdetail` ,`flevel` ,
  `fcreateorgid` ,`fuseorgid` ,`fforbidstatus` ,`fmasterid` ,`fissyspreset` ,`fdocumentstatus` ,
  `fcfitemid` ,`focfitemid` ,`fcfindirectitemid` ,`focfindirectitemid` ,`fallcurrency` ,`fcurrencylist` ,
  `fcurrencys` ,`fisshowjournal` ,`famountdc` ,`fiscontact` ,`company` ,`shujuzx` ,`refresh_jk` from erp_jd_ods.erp_jd_ods_fact_account_yc_cwzx
    union all 
    select `facctid` ,`fnumber` ,`fparentid` ,`fhelpercode` ,`fgroupid` ,`fdc` ,`faccttblid` ,`fiscash` ,`fisbank` ,
  `fisallocate` ,`fitemdetailid` ,`fisquantities` ,`funitgroupid` ,`funitid` ,`fisdetail` ,`flevel` ,
  `fcreateorgid` ,`fuseorgid` ,`fforbidstatus` ,`fmasterid` ,`fissyspreset` ,`fdocumentstatus` ,
  `fcfitemid` ,`focfitemid` ,`fcfindirectitemid` ,`focfindirectitemid` ,`fallcurrency` ,`fcurrencylist` ,
  `fcurrencys` ,`fisshowjournal` ,`famountdc` ,`fiscontact` ,`company` ,`shujuzx` ,`refresh_jk` from erp_jd_ods.erp_jd_ods_fact_account_wc01_cwzx;



