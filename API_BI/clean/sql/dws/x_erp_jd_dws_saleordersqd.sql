delete from erp_jd_dws.erp_jd_dws_saleordersqd
where riqi>=DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 63 DAY), '%Y-%m-01'); 


INSERT INTO erp_jd_dws.erp_jd_dws_saleordersqd (`riqi`,`kehuid` ,`kehumc` ,`xiaoshoubmdm` ,`xiaoshoubmmc` ,`danjulxmc` ,`wuliaobm` ,`wuliaomc` ,`jiashuihj_ac` ,`bumen_new` ,`bumen` ,`cost` ,`hanshuidj` ,`xiaoshousl_ac` ,`purchases_ac` ,`profit_ac` ,`company` ,`danjubh` )
SELECT  `riqi`,
`kehuid` ,
`kehumc` ,
bumenbm `xiaoshoubmdm` ,
bumenmc  `xiaoshoubmmc` ,
case when  `riqi` is not null then '销售出库单' end `danjulxmc` ,
`wuliaobm` ,
`wuliaomc` ,
jiashuihj `jiashuihj_ac` ,
`bumen_new` ,
`bumen` ,
`cost` ,
`hanshuidj` ,
shifasl `xiaoshousl_ac` ,
purchases `purchases_ac` ,
profit  `profit_ac` ,
`company` ,
`danjubh` 
FROM erp_jd_dwd.erp_jd_dwd_dim_saleshipping 
where bumen = '渠道'
and company = '杭州游卡文化创意有限公司'
and bumenmc like '%线下渠道部'
and wuliaomc not like '%三国杀英雄传%'
and kehumc not in 
('杭州迷思文化创意有限公司',
'展会',
'上海有杯咖啡有限公司',
'杭州泳淳网络技术有限公司',
'百世物流科技(中国)有限公司',
'广州市泓日商贸有限公司',
'京口区文趣心选百货经营部',
'其他',
'上海卡丫卡文化传播有限公司',
'上海卡卡丫文化传播有限公司',
'沈阳市沈河区尚品商业零售门市部'
) 
and riqi>=DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 63 DAY), '%Y-%m-01')

union all

SELECT  `riqi`,
`kehuid` ,
`kehumc` ,
bumenbm `xiaoshoubmdm` ,
bumenmc  `xiaoshoubmmc` ,
case when  `riqi` is not null then '销售出库单' end `danjulxmc` ,
`wuliaobm` ,
`wuliaomc` ,
jiashuihj `jiashuihj_ac` ,
`bumen_new` ,
`bumen` ,
`cost` ,
`hanshuidj` ,
shifasl `xiaoshousl_ac` ,
purchases `purchases_ac` ,
profit  `profit_ac` ,
`company` ,
`danjubh` 
FROM erp_jd_dwd.erp_jd_dwd_dim_saleshipping 
where bumen = '渠道'
and company = '杭州游卡文化创意有限公司'
and kehumc = 'YOKAKIDS快团团店'
and danjubh like 'SDO%'
and wuliaomc not like '%三国杀英雄传%'
and riqi>=DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 63 DAY), '%Y-%m-01')

union all

SELECT  `riqi`,
`kehuid` ,
`kehumc` ,
bumenbm `xiaoshoubmdm` ,
bumenmc  `xiaoshoubmmc` ,
case when  `riqi` is not null then '销售退货单' end `danjulxmc` ,
`wuliaobm` ,
`wuliaomc` ,
-jiashuihj `jiashuihj_ac` ,
`bumen_new` ,
`bumen` ,
`cost` ,
`hanshuidj` ,
-shifasl `xiaoshousl_ac` ,
-purchases `purchases_ac` ,
-profit  `profit_ac` ,
`company` ,
`danjubh` 
FROM erp_jd_dwd.erp_jd_dwd_dim_salereturn 
where bumen = '渠道'
and company = '杭州游卡文化创意有限公司'
and wuliaomc not like '%三国杀英雄传%'
and riqi>=DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 63 DAY), '%Y-%m-01')

union all

SELECT  `riqi`,
`kehuid` ,
`kehumc` ,
`xiaoshoubmdm` ,
`xiaoshoubmmc` ,
`danjulxmc` ,
`wuliaobm` ,
`wuliaomc` ,
jiashuihj `jiashuihj_ac` ,
`bumen_new` ,
`bumen` ,
`cost` ,
shijijshsdj  `hanshuidj` ,
hangbencijssl `xiaoshousl_ac` ,
purchases  `purchases_ac` ,
profit `profit_ac` ,
`company` ,
`danjubh`  
FROM erp_jd_dwd.erp_jd_dwd_dim_consignment 
where bumen = '渠道'
and company in ('杭州游卡文化创意有限公司','杭州迷思文化创意有限公司')
and wuliaomc not like '%三国杀英雄传%'
and riqi>=DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 63 DAY), '%Y-%m-01')

union all

SELECT  `riqi`,
	`kehuid` ,
	`kehumc` ,
	`xiaoshoubmdm` ,
	`xiaoshoubmmc` ,
	`danjulxmc` ,
	`wuliaobm` ,
	`wuliaomc` ,
	jiashuihj `jiashuihj_ac` ,
	`bumen_new` ,
	`bumen` ,
	`cost` ,
	`hanshuidj` ,
	xiaoshousl `xiaoshousl_ac` ,
	purchases `purchases_ac` ,
	profit `profit_ac` ,
	`company` ,
	`danjubh` 
FROM erp_jd_dwd.erp_jd_dwd_dim_saleorders 
where `danjulxmc` <>'寄售销售订单'
and bumen = '渠道'
and company = '杭州游卡文化创意有限公司'
and wuliaomc not like '%三国杀英雄传%'
and kehumc not in 
('北京诚宇动漫文化有限公司',
'广州奇乐动漫文化有限公司',
'杭州迷思文化创意有限公司',
'杭州泳淳网络技术有限公司',
'临沂弘龙文化科技有限公司',
'上海卡丫卡文化传播有限公司',
'上海卡卡丫文化传播有限公司',
'义乌市齐茜玩具商行',
'杭州蓝网文化传媒有限公司')
and riqi>=DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 63 DAY), '%Y-%m-01')
;