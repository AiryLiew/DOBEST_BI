drop table if exists xwj.xwj_jd_waicai;
CREATE TABLE xwj.xwj_jd_waicai( 
SELECT a.系列,
count(DISTINCT  `物料名称`) sku,
sum(`销量`) `销量`,
`近7日新物赏销量`,
`近7日现货销量`,
round(sum(`销量`)/count(DISTINCT  `物料名称`) ,0) 套数,
round(sum(`价税合计`),2) 收入,
round(sum(`含税成本`),2) 成本,
round(sum(`价税合计`)-sum(`含税成本`),2) 毛利,
round((1-sum(`含税成本`) /sum(`价税合计`))*100,1)  毛利率,
case when 客户 = '新物优选' then round((1-sum(`含税成本`) /sum(`价税合计`))*100,1) end 现货商城毛利率,
case when 客户 <> '新物优选' then round((1-sum(`含税成本`) /sum(`价税合计`))*100,1) end 电商毛利率,
min(日期) 最早销售日期,
max(日期) 最近销售日期,
库存sku,
库龄,
库存,
case when 库存 is null then datediff(max(日期),min(日期))+1 ELSE datediff(now(),min(日期)) end 动销天数
from(
SELECT `物料名称`,
SUBSTRING_INDEX(物料名称, '：', 1)  系列,
str_to_date(日期,'%Y-%m-%d') 日期,
       `季度`,
       `销量`,
       `价税合计`,
       `收入`,
       `单价`,
       `客户`,
       `年`,
       `月`,
       `是否滞销`,
       `含税成本`
FROM www_bi_ads.xwj_data
where 物料名称 like '电商JH%'
and 年  = year(date_sub(now(),interval 31 day))
and 客户 in ('新物优选','桌游志旗舰店','游卡桌游淘宝店','小红书-新物集','京东桌游志POP旗舰店')
) a 


left join(
SELECT 
SUBSTRING_INDEX(物料名称, '：', 1)  系列,
       sum(case when 客户 ='核心桌游盲盒' then `销量` end) `近7日新物赏销量`,
       sum(case when 客户 in ('新物优选','桌游志旗舰店','游卡桌游淘宝店','小红书-新物集','京东桌游志POP旗舰店') then `销量` end) `近7日现货销量`
FROM www_bi_ads.xwj_data
where 物料名称 like '电商JH%'
and 日期 >= date_sub(now(),interval 7 day)
and 客户 in ('新物优选','桌游志旗舰店','游卡桌游淘宝店','核心桌游盲盒','小红书-新物集','京东桌游志POP旗舰店')
group by SUBSTRING_INDEX(物料名称, '：', 1)
) c  on a.系列 = c.系列


left join (
SELECT 系列,
count(DISTINCT  `物料名称`) 库存sku,
max(doi) 库龄,
sum(surplus) 库存
from(
SELECT wuliaomc 物料名称,
SUBSTRING_INDEX(wuliaomc, '：', 1)  系列,
       surplus,
       doi
FROM erp_jd_dws.erp_jd_dws_doi_fc 
where wuliaomc like '电商JH%'
and cangkumc in ('下沙-新物集仓','下沙-核心桌游仓','义务-新物集仓','义务-核心桌游仓','松歌-新物集仓','松歌-核心桌游仓','义乌-新卡仓')
) a 
group by 系列
) b on a.系列 = b.系列

group by 系列
having sum(`价税合计`)>0
);