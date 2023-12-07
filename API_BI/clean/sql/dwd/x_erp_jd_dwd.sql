drop table if exists erp_jd_dwd.erp_jd_dwd_dim_acctagebalance;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_acctagebalance( 
    select * from erp_jd_ods.erp_jd_ods_dim_acctagebalance_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_acctagebalance_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_acctagebalance_xmgs
);







delete FROM erp_jd_dwd.erp_jd_dwd_dim_allocation
where riqi>=(
    SELECT DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 63 DAY), '%Y-%m-01') AS start_of_month
) ;

INSERT INTO erp_jd_dwd.erp_jd_dwd_dim_allocation(`fid`,`riqi` ,`danjubh` ,`danjuzt` ,`diaobofx` ,`wuliaobm` ,`wuliaomc` ,
  `guigexh` ,`danwei` ,`diaobosl`,`diaochuckid` ,`diaochuck` ,`diaoruckid` ,`diaoruck` ,`diaorubgzlx` ,`diaorubgz` , `diaorubgzmc` ,`diaochubgzlx` ,
  `diaochubgz` ,`diaochubgzmc` ,`guanlianxskh` ,`beizhu_bt` ,`beizhu_mx` ,`company` ,`refresh_jk` 
) 
select `fid`,`riqi` ,`danjubh` ,`danjuzt` ,`diaobofx` ,`wuliaobm` ,`wuliaomc` ,
`guigexh` ,`danwei` ,`diaobosl`,`diaochuckid` ,`diaochuck` ,`diaoruckid` ,`diaoruck` ,`diaorubgzlx` ,`diaorubgz` , `diaorubgzmc` ,`diaochubgzlx` ,
`diaochubgz` ,`diaochubgzmc` ,`guanlianxskh` ,`beizhu_bt` ,`beizhu_mx` ,`company` ,`refresh_jk` 
from erp_jd_ods.erp_jd_ods_dim_allocation_kyk_cwzx
where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

union all 
select `fid`,`riqi` ,`danjubh` ,`danjuzt` ,`diaobofx` ,`wuliaobm` ,`wuliaomc` ,
`guigexh` ,`danwei` ,`diaobosl`,`diaochuckid` ,`diaochuck` ,`diaoruckid` ,`diaoruck` ,`diaorubgzlx` ,`diaorubgz` , `diaorubgzmc` ,`diaochubgzlx` ,
`diaochubgz` ,`diaochubgzmc` ,`guanlianxskh` ,`beizhu_bt` ,`beizhu_mx` ,`company` ,`refresh_jk`  
from erp_jd_ods.erp_jd_ods_dim_allocation_ms_cwzx
where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

union all 
select `fid`,`riqi` ,`danjubh` ,`danjuzt` ,`diaobofx` ,`wuliaobm` ,`wuliaomc` ,
`guigexh` ,`danwei` ,`diaobosl`,`diaochuckid` ,`diaochuck` ,`diaoruckid` ,`diaoruck` ,`diaorubgzlx` ,`diaorubgz` , `diaorubgzmc` ,`diaochubgzlx` ,
`diaochubgz` ,`diaochubgzmc` ,`guanlianxskh` ,`beizhu_bt` ,`beizhu_mx` ,`company` ,`refresh_jk`  
from erp_jd_ods.erp_jd_ods_dim_allocation_wc_cwzx
where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

union all 
select `fid`,`riqi` ,`danjubh` ,`danjuzt` ,`diaobofx` ,`wuliaobm` ,`wuliaomc` ,
`guigexh` ,`danwei` ,`diaobosl`,`diaochuckid` ,`diaochuck` ,`diaoruckid` ,`diaoruck` ,`diaorubgzlx` ,`diaorubgz` , `diaorubgzmc` ,`diaochubgzlx` ,
`diaochubgz` ,`diaochubgzmc` ,`guanlianxskh` ,`beizhu_bt` ,`beizhu_mx` ,`company` ,`refresh_jk`  
from erp_jd_ods.erp_jd_ods_dim_allocation_wc01_cwzx
where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

union all 
select `fid`,`riqi` ,`danjubh` ,`danjuzt` ,`diaobofx` ,`wuliaobm` ,`wuliaomc` ,
`guigexh` ,`danwei` ,`diaobosl`,`diaochuckid` ,`diaochuck` ,`diaoruckid` ,`diaoruck` ,`diaorubgzlx` ,`diaorubgz` , `diaorubgzmc` ,`diaochubgzlx` ,
`diaochubgz` ,`diaochubgzmc` ,`guanlianxskh` ,`beizhu_bt` ,`beizhu_mx` ,`company` ,`refresh_jk`  
from erp_jd_ods.erp_jd_ods_dim_allocation_yc_cwzx
where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01') 
;








delete FROM erp_jd_dwd.erp_jd_dwd_dim_assemble
where rukurq>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01');

INSERT INTO erp_jd_dwd.erp_jd_dwd_dim_assemble( `fid`,`rukurq`,`shiwulx`,`wuliaobm`,`wuliaomc`,`wuliaolbdm`,
`wuliaolbmc`,`shuliang`,`cangkuid`,`cangkumc`,`danjubh`,`fdetailid`,`company`,`refresh_jk`
)
select `fid`,`rukurq`,`shiwulx`,`wuliaobm`,`wuliaomc`,`wuliaolbdm`,
`wuliaolbmc`,`shuliang`,`cangkuid`,`cangkumc`,`danjubh`,`fdetailid`,`company`,`refresh_jk` 
from erp_jd_ods.erp_jd_ods_dim_assemble_kyk_cwzx
where rukurq>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

union all 
select `fid`,`rukurq`,`shiwulx`,`wuliaobm`,`wuliaomc`,`wuliaolbdm`,
`wuliaolbmc`,`shuliang`,`cangkuid`,`cangkumc`,`danjubh`,`fdetailid`,`company`,`refresh_jk` 
from erp_jd_ods.erp_jd_ods_dim_assemble_wc_cwzx
where rukurq>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

union all 
select `fid`,`rukurq`,`shiwulx`,`wuliaobm`,`wuliaomc`,`wuliaolbdm`,
`wuliaolbmc`,`shuliang`,`cangkuid`,`cangkumc`,`danjubh`,`fdetailid`,`company`,`refresh_jk` 
from erp_jd_ods.erp_jd_ods_dim_assemble_wc01_cwzx
where rukurq>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

union all 
select `fid`,`rukurq`,`shiwulx`,`wuliaobm`,`wuliaomc`,`wuliaolbdm`,
`wuliaolbmc`,`shuliang`,`cangkuid`,`cangkumc`,`danjubh`,`fdetailid`,`company`,`refresh_jk` 
from erp_jd_ods.erp_jd_ods_dim_assemble_yc_cwzx
where rukurq>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')
;






drop table if exists erp_jd_dwd.erp_jd_dwd_dim_balance;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_balance( 
    select * from erp_jd_ods.erp_jd_ods_dim_balance_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_balance_xmgs
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_balance_dobest
);







delete FROM erp_jd_dwd.erp_jd_dwd_dim_distributedin
where riqi >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01');

INSERT INTO erp_jd_dwd.erp_jd_dwd_dim_distributedin (  
`fid`, `riqi`, `danjubh`, `danjuzt`, `wuliaobm`, `wuliaomc`, `guigexh`,  
`danwei`, `diaorusl`, `diaochuckid`, `diaochuck`, `diaoruckid`, `diaoruck`,  
`company`, `refresh_jk`  
)  
SELECT   
`fid`, `riqi`, `danjubh`, `danjuzt`, `wuliaobm`, `wuliaomc`, `guigexh`,  
`danwei`, `diaorusl`, `diaochuckid`, `diaochuck`, `diaoruckid`, `diaoruck`,  
`company`, `refresh_jk`  
FROM erp_jd_ods.erp_jd_ods_dim_distributedin_wc_cwzx  
WHERE riqi >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')
;








delete FROM erp_jd_dwd.erp_jd_dwd_dim_distributedout
where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01');

INSERT INTO erp_jd_dwd.erp_jd_dwd_dim_distributedout(`fid` ,`riqi` ,`danjubh` ,`danjuzt` ,`wuliaobm` ,`wuliaomc` ,`guigexh` ,
`danwei` ,`diaochusl` ,`diaochuckid` ,`diaochuck` ,`diaoruckid` ,`diaoruck` ,`company` ,`refresh_jk`
) 
select `fid` ,`riqi` ,`danjubh` ,`danjuzt` ,`wuliaobm` ,`wuliaomc` ,`guigexh` ,
`danwei` ,`diaochusl` ,`diaochuckid` ,`diaochuck` ,`diaoruckid` ,`diaoruck` ,`company` ,`refresh_jk` 
from erp_jd_ods.erp_jd_ods_dim_distributedout_wc_cwzx
where riqi >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01');









delete FROM erp_jd_dwd.erp_jd_dwd_dim_inventoryloss
where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01');

INSERT INTO erp_jd_dwd.erp_jd_dwd_dim_inventoryloss( `fid` ,`riqi` ,`bumendm` ,`bumenmc` ,`wuliaobm` ,
`wuliaomc` ,`cangkuid` ,`cangkumc` ,`pankuisl` ,`danjubh` ,`company` ,`refresh_jk` 
)
select `fid` ,`riqi` ,`bumendm` ,`bumenmc` ,`wuliaobm` ,
`wuliaomc` ,`cangkuid` ,`cangkumc` ,`pankuisl` ,`danjubh` ,`company` ,`refresh_jk` 
from erp_jd_ods.erp_jd_ods_dim_inventoryloss_wc_cwzx
where riqi >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

union all 
select `fid` ,`riqi` ,`bumendm` ,`bumenmc` ,`wuliaobm` ,
`wuliaomc` ,`cangkuid` ,`cangkumc` ,`pankuisl` ,`danjubh` ,`company` ,`refresh_jk` 
from erp_jd_ods.erp_jd_ods_dim_inventoryloss_yc_cwzx
where riqi >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

union all 
select `fid` ,`riqi` ,`bumendm` ,`bumenmc` ,`wuliaobm` ,
`wuliaomc` ,`cangkuid` ,`cangkumc` ,`pankuisl` ,`danjubh` ,`company` ,`refresh_jk` 
from erp_jd_ods.erp_jd_ods_dim_inventoryloss_wc01_cwzx
where riqi >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')
;









delete FROM erp_jd_dwd.erp_jd_dwd_dim_inventoryprofit
where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01');

INSERT INTO erp_jd_dwd.erp_jd_dwd_dim_inventoryprofit( `fid` ,`riqi` ,`bumendm` ,`bumenmc` ,`wuliaobm` ,
`wuliaomc` ,`cangkuid` ,`cangkumc` ,`panyingsl` ,`danjubh` ,`company` ,`refresh_jk`
) 
select `fid` ,`riqi` ,`bumendm` ,`bumenmc` ,`wuliaobm` ,
`wuliaomc` ,`cangkuid` ,`cangkumc` ,`panyingsl` ,`danjubh` ,`company` ,`refresh_jk` 
from erp_jd_ods.erp_jd_ods_dim_inventoryprofit_wc_cwzx
where riqi >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

union all 
select `fid` ,`riqi` ,`bumendm` ,`bumenmc` ,`wuliaobm` ,
`wuliaomc` ,`cangkuid` ,`cangkumc` ,`panyingsl` ,`danjubh` ,`company` ,`refresh_jk` 
from erp_jd_ods.erp_jd_ods_dim_inventoryprofit_yc_cwzx
where riqi >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')
;








delete FROM erp_jd_dwd.erp_jd_dwd_dim_othersreceiving
where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01');

INSERT INTO erp_jd_dwd.erp_jd_dwd_dim_othersreceiving( `fid` ,`riqi` ,`bumendm` ,`bumenmc` ,`wuliaobm` ,`wuliaomc` ,`wuliaofzid` ,
`wuliaofzmc` ,`cangkuid` ,`cangkumc` ,`shishousl`,`danjubh` ,`company` ,`refresh_jk`,`year` ,`month`
)
select `fid` ,`riqi` ,`bumendm` ,`bumenmc` ,`wuliaobm` ,`wuliaomc` ,`wuliaofzid` ,
`wuliaofzmc` ,`cangkuid` ,`cangkumc` ,`shishousl`,`danjubh` ,`company` ,`refresh_jk`,year(riqi) `year`,month(riqi) `month` from erp_jd_ods.erp_jd_ods_dim_othersreceiving_kyk_cwzx
where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')  

union all 
select `fid` ,`riqi` ,`bumendm` ,`bumenmc` ,`wuliaobm` ,`wuliaomc` ,`wuliaofzid` ,
`wuliaofzmc` ,`cangkuid` ,`cangkumc` ,`shishousl`,`danjubh` ,`company` ,`refresh_jk`,year(riqi) `year`,month(riqi) `month` from erp_jd_ods.erp_jd_ods_dim_othersreceiving_wc_cwzx
where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

union all 
select `fid` ,`riqi` ,`bumendm` ,`bumenmc` ,`wuliaobm` ,`wuliaomc` ,`wuliaofzid` ,
`wuliaofzmc` ,`cangkuid` ,`cangkumc` ,`shishousl`,`danjubh` ,`company` ,`refresh_jk`,year(riqi) `year`,month(riqi) `month` from erp_jd_ods.erp_jd_ods_dim_othersreceiving_wc01_cwzx
where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

union all 
select `fid` ,`riqi` ,`bumendm` ,`bumenmc` ,`wuliaobm` ,`wuliaomc` ,`wuliaofzid` ,
`wuliaofzmc` ,`cangkuid` ,`cangkumc` ,`shishousl`,`danjubh` ,`company` ,`refresh_jk`,year(riqi) `year`,month(riqi) `month` from erp_jd_ods.erp_jd_ods_dim_othersreceiving_yc_cwzx
where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')
;









delete FROM erp_jd_dwd.erp_jd_dwd_dim_othersshipping
where shujuzx = '财务数据中心';

INSERT INTO erp_jd_dwd.erp_jd_dwd_dim_othersshipping(`wuliaobm` ,`wuliaomc` ,`shifasl`,`bumenbm` ,`bumenmc` ,`wuliaofzid` ,`wuliaofzmc` ,
`riqi` ,`shenhezt` ,`cangkuid` ,`cangkumc` ,`danjubh` ,`lingliaolxid` ,`lingliaolxmc` ,`beizhu` ,`company` ,`shujuzx`,`refresh_jk` ,`year` ,`month`
)   
select `wuliaobm` ,`wuliaomc` ,`shifasl`,`bumenbm` ,`bumenmc` ,`wuliaofzid` ,`wuliaofzmc` ,
`riqi` ,`shenhezt` ,`cangkuid` ,`cangkumc` ,`danjubh` ,`lingliaolxid` ,`lingliaolxmc` ,`beizhu` ,`company` ,`shujuzx`,`refresh_jk` ,year(riqi) `year`,month(riqi) `month` from erp_jd_ods.erp_jd_ods_dim_othersshipping_kyk_cwzx
where shenhezt = '已审核'

union all 
select `wuliaobm` ,`wuliaomc` ,`shifasl`,`bumenbm` ,`bumenmc` ,`wuliaofzid` ,`wuliaofzmc` ,
`riqi` ,`shenhezt` ,`cangkuid` ,`cangkumc` ,`danjubh` ,`lingliaolxid` ,`lingliaolxmc` ,`beizhu` ,`company` ,`shujuzx`,`refresh_jk` ,year(riqi) `year`,month(riqi) `month` from erp_jd_ods.erp_jd_ods_dim_othersshipping_ms_cwzx
where shenhezt = '已审核'

union all 
select `wuliaobm` ,`wuliaomc` ,`shifasl`,`bumenbm` ,`bumenmc` ,`wuliaofzid` ,`wuliaofzmc` ,
`riqi` ,`shenhezt` ,`cangkuid` ,`cangkumc` ,`danjubh` ,`lingliaolxid` ,`lingliaolxmc` ,`beizhu` ,`company` ,`shujuzx`,`refresh_jk` ,year(riqi) `year`,month(riqi) `month` from erp_jd_ods.erp_jd_ods_dim_othersshipping_wc_cwzx
where shenhezt = '已审核'

union all 
select `wuliaobm` ,`wuliaomc` ,`shifasl`,`bumenbm` ,`bumenmc` ,`wuliaofzid` ,`wuliaofzmc` ,
`riqi` ,`shenhezt` ,`cangkuid` ,`cangkumc` ,`danjubh` ,`lingliaolxid` ,`lingliaolxmc` ,`beizhu` ,`company` ,`shujuzx`,`refresh_jk` ,year(riqi) `year`,month(riqi) `month` from erp_jd_ods.erp_jd_ods_dim_othersshipping_wc01_cwzx
where shenhezt = '已审核'

union all
select `wuliaobm` ,`wuliaomc` ,`shifasl`,`bumenbm` ,`bumenmc` ,`wuliaofzid` ,`wuliaofzmc` ,
`riqi` ,`shenhezt` ,`cangkuid` ,`cangkumc` ,`danjubh` ,`lingliaolxid` ,`lingliaolxmc` ,`beizhu` ,`company` ,`shujuzx`,`refresh_jk` ,year(riqi) `year`,month(riqi) `month` from erp_jd_ods.erp_jd_ods_dim_othersshipping_yc_cwzx
where shenhezt = '已审核' ;









drop table if exists erp_jd_dwd.erp_jd_dwd_dim_prepayment;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_prepayment( 
    select * from erp_jd_ods.erp_jd_ods_dim_prepayment_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_prepayment_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_prepayment_xmgs
);








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








drop table if exists erp_jd_dwd.erp_jd_dwd_dim_voucherentry;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_voucherentry( 
    select * from erp_jd_ods.erp_jd_ods_dim_voucherentry_cwzx
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_voucherentry_dobest
    union all 
    select * from erp_jd_ods.erp_jd_ods_dim_voucherentry_xmgs
);








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
