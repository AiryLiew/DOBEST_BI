drop table if exists erp_jd_dws.erp_jd_dws_saleship_return;
CREATE TABLE erp_jd_dws.erp_jd_dws_saleship_return (
    SELECT  `company` ,
      `year` ,
      `month` ,
      `riqi`,
      `bumen` ,
      `bumenmc` ,
      `bumenbm` ,
      `kehuid` ,
      `kehumc` ,
      `danjubh` ,
      `shifouzp` ,
      `cangkumc` ,
      `cangkuid` ,
      `wuliaofzid` ,
      `wuliaofzmc` ,
      `wuliaobm` ,
      `wuliaomc` ,
      -`shifasl` shifasl,
      `hanshuidj` ,
      -`jiashuihj` jiashuihj,
      -`profit` profit,
      `cost`,
      -`purchases` purchases
    FROM erp_jd_dwd.erp_jd_dwd_dim_salereturn 

    union all

    SELECT  `company` ,
      `year` ,
      `month` ,
      `riqi`,
      `bumen` ,
      `bumenmc` ,
      `bumenbm` ,
      `kehuid` ,
      `kehumc` ,
      `danjubh` ,
      `shifouzp` ,
      `cangkumc` ,
      `cangkuid` ,
      `wuliaofzid` ,
      `wuliaofzmc` ,
      `wuliaobm` ,
      `wuliaomc` ,
      `shifasl` ,
      `hanshuidj` ,
      `jiashuihj` ,
      `profit` ,
      `cost` ,
      `purchases`
    FROM erp_jd_dwd.erp_jd_dwd_dim_saleshipping 
);