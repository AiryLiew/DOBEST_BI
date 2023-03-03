drop table if exists erp_jd_ads.`key_product_sales_area`;
CREATE TABLE erp_jd_ads.`key_product_sales_area` (
SELECT 
b.businessarea 部门,
case when x.kehumc in ('游卡桌游文化店',
 '三国杀游卡专卖店（京东）',
 '周边商城',
 '桌游志旗舰店',
 '北京京东世纪贸易有限公司',
 '三国杀旗舰店',
 '广州大蓝网络科技有限公司',
 '上海弘贯网络技术有限公司',
 '网易（杭州）网络有限公司',
 '小红书-YOKAGAMES',
 '拼多多-YOKAGAMES游卡专卖店',
 '拼多多- 三国杀官方旗舰店',
 '剧本杀拼多多店',
 '剧本杀旗舰店',
 '京东-剧本杀旗舰店',
 '真相档案旗舰店',
 '卡丫卡直播间',
 '剧本杀得物店',
 '抖店-剧本杀了谁',
 '三国杀天猫超市',
 '三国杀快手小店') then '电商' else b.kehufzmc end 业务区域,
x.kehumc 客户,
b.`name_prov1` 省1,
b.`name_city1` 市1,
b.`name_prov` 省,
b.`name_city` 市,
a.classify 产品大类,
a.classify_1 产品中类,
a.classify_2 产品小类,
x.wuliaomc 产品名称,
year(x.riqi) 年,
quarter(x.riqi) 季度,
month(x.riqi) 月,
week(x.riqi) 周, 
x.riqi 日期,
sum(x.shifasl) 总销量 ,
sum(x.jiashuihj) 总销售额,
sum(x.profit) 毛利

FROM erp_jd_dwd.erp_jd_dwd_dim_saleshipping x

left join(
SELECT classify,classify_1,classify_2,wuliaomc 
FROM erp_jd_dwd.erp_jd_dwd_fact_classify 
) a on x.wuliaomc = a.wuliaomc


left join(
SELECT kehumc,
kehufzmc,
businessarea,
`name_prov1`,
  `name_city1`,
  `name_prov` ,
  `name_city`
FROM erp_jd_dwd.erp_jd_dwd_fact_client 
) b on x.kehumc = b.kehumc

where x.kehumc not in ('杭州泳淳网络技术有限公司','杭州游卡文化创意有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司')

group by x.bumen_new,
x.kehumc,
x.wuliaomc,
x.riqi );