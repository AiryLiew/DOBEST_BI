delete from erp_jd_dws.erp_jd_dws_saleship_return
where riqi>=(
	SELECT DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 63 DAY), '%Y-%m-01') AS start_of_month
);

INSERT INTO erp_jd_dws.erp_jd_dws_saleship_return(`company` ,`year` ,`month` ,`riqi`,`bumen` ,`bumenmc` ,`bumenbm` ,`kehuid` ,`kehumc` ,`danjubh` ,`shifouzp` ,`cangkumc` ,`cangkuid` ,`wuliaofzid` ,`wuliaofzmc` ,`wuliaobm` ,`wuliaomc` ,shifasl,`hanshuidj` ,jiashuihj,profit,`cost`,purchases) 
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
where riqi>=(
	SELECT DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 63 DAY), '%Y-%m-01') AS start_of_month
)

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
where riqi>=(
	SELECT DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 63 DAY), '%Y-%m-01') AS start_of_month
);
                                        
                                        
                         