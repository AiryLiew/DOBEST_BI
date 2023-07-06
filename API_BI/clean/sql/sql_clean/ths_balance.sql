drop table if exists www_bi_ads.ccwl_fee_saleorders;
CREATE TABLE www_bi_ads.ccwl_fee_saleorders( 
  SELECT 
  a.*,
-- b.物料masterid,
-- b. 仓库,
-- b.仓位,
  b.*,
c.核算结束时间,
c.核算开始时间,
c.日期,
c.会计期间,
c.会计年,
c.年期,
c.fdimensionid,
case when c.fdimensionid = 4 then '杭州游卡文化创意有限公司'
      when c.fdimensionid = 1 then '杭州迷思文化创意有限公司' 
      when c.fdimensionid = 5 then '杭州泳淳网络技术有限公司' 
      when c.fdimensionid = 14 then '杭州游卡文化创意有限公司拱墅区分公司' 
      when c.fdimensionid = 9 then '上海卡丫卡文化传播有限公司' 
      else null end 账簿

FROM (
SELECT  
   fdimeentryid,
   fid,
  `fendinitkey` 	期初期末标识,
  `famount` 金额,
  `fbeginadjamount` 期初金额排除期初调整的金额,
  `fcurrentinamount` 本期入金额,
  `fcurrentinqty` 本期入数量,
  `fcurrentoutamount` 本期出金额,
  `foutstockqty` 本期出库数量,
  `fqty` 数量,
  `fyearoutsumamount` 	本期累计出金额,
  `fyearoutsumqty` 	本年累计出数量,
  `fyearsumamount` 本年累计金额,
  `fyearsumqty` 	本年累计数量
FROM erp_jd_ods.erp_jd_ods_dim_ths_inivbalanceh_cwzx 

union all

SELECT  
   fdimeentryid,
   fid,
  `fendinitkey` 	期初期末标识,
  `famount` 金额,
  `fbeginadjamount` 期初金额排除期初调整的金额,
  `fcurrentinamount` 本期入金额,
  `fcurrentinqty` 本期入数量,
  `fcurrentoutamount` 本期出金额,
  `foutstockqty` 本期出库数量,
  `fqty` 数量,
  `fyearoutsumamount` 	本期累计出金额,
  `fyearoutsumqty` 	本年累计出数量,
  `fyearsumamount` 本年累计金额,
  `fyearsumqty` 	本年累计数量
FROM erp_jd_ods.erp_jd_ods_dim_ths_inivbalance_cwzx 
) a

left join(
SELECT `fentryid` ,
  `facctgrangeid` ,
  `fauxpropid` ,
  `fbomid` ,
  `fisenable` ,
  `flotnumber` ,
  `fmasterid` 物料masterid,
  `fmtono` ,
  `fprojectno` ,
  `fstockid` 仓库,
  `fstocklocid` 仓位,
  `fstockstate` ,
  `fvaluationmethod`
FROM erp_jd_ods.erp_jd_ods_dim_ths_stockdimension_cwzx
) b on a.fdimeentryid = b.fentryid

left join(
SELECT `fid` ,
  `facctgenddate` 	核算结束时间,
  `facctgschedule` ,
  `facctgstartdate` 	核算开始时间,
  `fchckoutdate` ,
  `fcomputeid` ,
  `fdate` 日期,
  `fdimensionid` ,
  `finestimatedate` ,
  `finventoryacctedate` ,
  `fisacctgforover` ,
  `fisacctging` ,
  `fisseriouserr` ,
  `fperiod` 会计期间,
  `fyear` 会计年,
  `fyearperiod` 年期
FROM erp_jd_ods.erp_jd_ods_dim_outacctg_cwzx 
) c on a.fid = c.fid

);