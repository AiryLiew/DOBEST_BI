delete FROM erp_jd_dwd.erp_jd_dwd_dim_allocation
where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01') ;

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

union all 
select `fid`,`riqi` ,`danjubh` ,`danjuzt` ,`diaobofx` ,`wuliaobm` ,`wuliaomc` ,
`guigexh` ,`danwei` ,`diaobosl`,`diaochuckid` ,`diaochuck` ,`diaoruckid` ,`diaoruck` ,`diaorubgzlx` ,`diaorubgz` , `diaorubgzmc` ,`diaochubgzlx` ,
`diaochubgz` ,`diaochubgzmc` ,`guanlianxskh` ,`beizhu_bt` ,`beizhu_mx` ,`company` ,`refresh_jk` 
from erp_jd_ods.erp_jd_ods_dim_allocation_kky_hz_cwzx
where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01') 

union all 
select `fid`,`riqi` ,`danjubh` ,`danjuzt` ,`diaobofx` ,`wuliaobm` ,`wuliaomc` ,
`guigexh` ,`danwei` ,`diaobosl`,`diaochuckid` ,`diaochuck` ,`diaoruckid` ,`diaoruck` ,`diaorubgzlx` ,`diaorubgz` , `diaorubgzmc` ,`diaochubgzlx` ,
`diaochubgz` ,`diaochubgzmc` ,`guanlianxskh` ,`beizhu_bt` ,`beizhu_mx` ,`company` ,`refresh_jk`  
from erp_jd_ods.erp_jd_ods_dim_allocation_fzh_cwzx
where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')
;








delete FROM erp_jd_dwd.erp_jd_dwd_dim_assemble
where rukurq>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01');

INSERT INTO erp_jd_dwd.erp_jd_dwd_dim_assemble( `fid`,`rukurq`,`shiwulx`,`wuliaobm`,`wuliaomc`,`wuliaolbdm`,
`wuliaolbmc`,`shuliang`,`cangkuid`,`cangkumc`,`danjubh`,`chuangjianrid`,`chuangjianrmc`,`fdetailid`,`danweibm`,`danweimc`,`chengbenj`,`company`,`refresh_jk`
)
select `fid`,`rukurq`,`shiwulx`,`wuliaobm`,`wuliaomc`,`wuliaolbdm`,
`wuliaolbmc`,`shuliang`,`cangkuid`,`cangkumc`,`danjubh`,`chuangjianrid`,`chuangjianrmc`,`fdetailid`,`danweibm`,`danweimc`,`chengbenj`,`company`,`refresh_jk`
from erp_jd_ods.erp_jd_ods_dim_assemble_kyk_cwzx
where rukurq>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

union all 
select `fid`,`rukurq`,`shiwulx`,`wuliaobm`,`wuliaomc`,`wuliaolbdm`,
`wuliaolbmc`,`shuliang`,`cangkuid`,`cangkumc`,`danjubh`,`chuangjianrid`,`chuangjianrmc`,`fdetailid`,`danweibm`,`danweimc`,`chengbenj`,`company`,`refresh_jk`
from erp_jd_ods.erp_jd_ods_dim_assemble_wc_cwzx
where rukurq>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')

union all 
select `fid`,`rukurq`,`shiwulx`,`wuliaobm`,`wuliaomc`,`wuliaolbdm`,
`wuliaolbmc`,`shuliang`,`cangkuid`,`cangkumc`,`danjubh`,`chuangjianrid`,`chuangjianrmc`,`fdetailid`,`danweibm`,`danweimc`,`chengbenj`,`company`,`refresh_jk`
from erp_jd_ods.erp_jd_ods_dim_assemble_wc01_cwzx
where rukurq>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')
and danjubh<>'新物集改负库存用'

union all 
select `fid`,`rukurq`,`shiwulx`,`wuliaobm`,`wuliaomc`,`wuliaolbdm`,
`wuliaolbmc`,`shuliang`,`cangkuid`,`cangkumc`,`danjubh`,`chuangjianrid`,`chuangjianrmc`,`fdetailid`,`danweibm`,`danweimc`,`chengbenj`,`company`,`refresh_jk`
from erp_jd_ods.erp_jd_ods_dim_assemble_yc_cwzx
where rukurq>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')
;









-- delete FROM erp_jd_dwd.erp_jd_dwd_dim_distributedin
-- where riqi >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01');

-- INSERT INTO erp_jd_dwd.erp_jd_dwd_dim_distributedin (  
-- `fid`, `riqi`, `danjubh`, `danjuzt`, `wuliaobm`, `wuliaomc`, `guigexh`,  
-- `danwei`, `diaorusl`, `diaochuckid`, `diaochuck`, `diaoruckid`, `diaoruck`,  
-- `company`, `refresh_jk`  
-- )  
-- SELECT   
-- `fid`, `riqi`, `danjubh`, `danjuzt`, `wuliaobm`, `wuliaomc`, `guigexh`,  
-- `danwei`, `diaorusl`, `diaochuckid`, `diaochuck`, `diaoruckid`, `diaoruck`,  
-- `company`, `refresh_jk`  
-- FROM erp_jd_ods.erp_jd_ods_dim_distributedin_wc_cwzx  
-- WHERE riqi >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')
-- ;








-- delete FROM erp_jd_dwd.erp_jd_dwd_dim_distributedout
-- where riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01');

-- INSERT INTO erp_jd_dwd.erp_jd_dwd_dim_distributedout(`fid` ,`riqi` ,`danjubh` ,`danjuzt` ,`wuliaobm` ,`wuliaomc` ,`guigexh` ,
-- `danwei` ,`diaochusl` ,`diaochuckid` ,`diaochuck` ,`diaoruckid` ,`diaoruck` ,`company` ,`refresh_jk`
-- ) 
-- select `fid` ,`riqi` ,`danjubh` ,`danjuzt` ,`wuliaobm` ,`wuliaomc` ,`guigexh` ,
-- `danwei` ,`diaochusl` ,`diaochuckid` ,`diaochuck` ,`diaoruckid` ,`diaoruck` ,`company` ,`refresh_jk` 
-- from erp_jd_ods.erp_jd_ods_dim_distributedout_wc_cwzx
-- where riqi >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01')
-- and danjubh <> 'FBDC00000884';









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
and `danjubh`<>'QTRK0001244'

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
`riqi` ,`shenhezt` ,`cangkuid` ,`cangkumc` ,`danjubh` ,`lingliaolxid` ,`lingliaolxmc` ,`beizhu` ,`company` ,`shujuzx`,`refresh_jk` ,year(riqi) `year`,month(riqi) `month` from erp_jd_ods.erp_jd_ods_dim_othersshipping_kky_hz_cwzx
where shenhezt = '已审核'

union all
select `wuliaobm` ,`wuliaomc` ,`shifasl`,`bumenbm` ,`bumenmc` ,`wuliaofzid` ,`wuliaofzmc` ,
`riqi` ,`shenhezt` ,`cangkuid` ,`cangkumc` ,`danjubh` ,`lingliaolxid` ,`lingliaolxmc` ,`beizhu` ,`company` ,`shujuzx`,`refresh_jk` ,year(riqi) `year`,month(riqi) `month` from erp_jd_ods.erp_jd_ods_dim_othersshipping_yc_cwzx
where shenhezt = '已审核' ;

