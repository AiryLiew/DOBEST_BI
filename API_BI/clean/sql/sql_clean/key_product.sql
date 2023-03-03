drop table if exists erp_jd_ads.`key_product`;
CREATE TABLE erp_jd_ads.`key_product` (
select a.`classify` `产品大类`  ,
a.`classify_1` `产品中类`  ,
a.`classify_2` `产品小类`  ,
a.`wuliaomc` `物料名称` ,
date_format(b.`riqi`,'%Y-%m-%d') `上市日期`,
date_format(c.`第一次出库时间`  ,'%Y-%m-%d') `第一次出库时间`,
date_format(c.`最近一次出库时间`  ,'%Y-%m-%d') `最近一次出库时间`,
round(d.`cost`,2) `采购成本`  ,
case when a.`wuliaomc` in ('真相档案J1-灵视·血月','真相档案J1-轮回客栈') then e.`采购数量`*2 else e.`采购数量` end `采购数量`  ,
n.`首批采购数量`  ,
case when a.`wuliaomc` is not null then 0 end `首批采购清空时间`  ,
case when a.`wuliaomc` is not null then 0 end`首批采购销售周期`  ,
e.`采购下单次数`  ,
o.`客户下单次数`  ,
case when a.`wuliaomc` is not null then 0 end `下单次数(2次及以上)客户单数`  ,
case when a.`wuliaomc` is not null then 0 end `下单次数(2次及以上)客户数`  ,
round(e.`总投入`,2) `总投入`,
round(e.`采购数量`*c.`平均售价`,2)  `预计产出`  , 
f.receiving `期初库存`  ,
ifnull(g.`采购入库数量`,0)-ifnull(p.`采购退料`,0)  `采购入库数量`,
case when ifnull(g.`采购入库数量`,0)-ifnull(p.`采购退料`,0)-ifnull(e.`采购数量`,0)>0 then ifnull(g.`采购入库数量`,0)-ifnull(p.`采购退料`,0)-ifnull(e.`采购数量`,0) else 0 end `入库赠品数量` ,
c.`文创业务销量` ,
c.`其他公司业务销量` ,
h.`公司领料`  ,
ifnull(i.`其他入库`,0)-ifnull(h.`其他出库`,0) `其他出入库` ,
ifnull(c.`总销量`,0) - ifnull(k.`退货`,0) `销量合计`  ,
c.`赠品数量`  ,
round((ifnull(c.`总销量`,0) - ifnull(k.`退货`,0))/(ifnull(g.`采购入库数量`,0)-ifnull(p.`采购退料`,0) + ifnull(f.`receiving`,0)),2) `实际动销率`,
round(1-j.`库存结余`/(ifnull(g.`采购入库数量`,0)-ifnull(p.`采购退料`,0) + ifnull(f.`receiving`,0)),2) `动销率`,
round(c.`总销售成本`,2)  `总销售成本`,
round(c.`总销售额`,2)  `总销售额`,
round(c.`平均售价`,2)  `平均售价`,
round(c.`已售产品毛利`,2)  `已售产品毛利`,
round(ifnull(c.`总销售额`,0) - ifnull(e.`总投入`,0),2) `总盈亏` ,
round(c.`已售产品毛利`/c.`总销售额`,2) `毛利率`  ,
ifnull(j.`库存结余`,0)-(ifnull(f.`receiving`,0)+ifnull(g.`采购入库数量`,0)-ifnull(p.`采购退料`,0)-ifnull(c.`总销量`,0) + ifnull(k.`退货`,0)-ifnull(h.`公司领料`,0)+ifnull(i.`其他入库`,0)-ifnull(h.`其他出库`,0)) `库存调整入库` ,
j.`库存结余`  ,
j.`库存结余（不含残次仓）`  ,
j.`库存结余（产品管理部统计）`  ,
k.`退货`  ,
round(k.`退货`/(ifnull(c.`文创业务销量`,0)+ifnull(c.`其他公司业务销量`,0)),2)  `退货率`  ,
e.`工厂结余`  ,
case when l.`现货可发`>e.`工厂结余` then 0 else l.`现货可发` end `现货可发`  ,
ifnull(j.`库存结余`,0)+ifnull(e.`工厂结余`,0)  `库存结余+工厂结余` ,
ifnull(j.`库存结余`,0)+ifnull(l.`现货可发`,0)  `库存结余+现货可发` ,
m.`0-180天`  ,
m.`180-360天`  ,
m.`360天以上`  ,
round(d.`cost`*j.`库存结余`,2) `剩余产品价值`  ,
round(d.`cost`*j.`库存结余（不含残次仓）`,2) `剩余产品价值（不含残次仓）`,
round(d.`cost`*j.`库存结余（产品管理部统计）`,2) `剩余产品价值（产品管理部统计）`,
round(d.`cost`*e.`工厂结余`,2)  `剩余产品价值（工厂）`  ,
c.`渠道销量` ,
c.`电商销量` ,
c.`近一年销量` ,
round(c.`总销量`/datediff(c.`最近一次出库时间`,c.`第一次出库时间`)*30,0) `月均销量`  ,
case when datediff(current_date(), c.`最近一次出库时间`)>=90 and j.`库存结余`=0 then '近三月未出货且无库存' else '正常' end `标记`
FROM erp_jd_dwd.erp_jd_dwd_fact_classify a

left join(
SELECT wlmc_all,riqi 
FROM erp_jd_dws.erp_jd_dws_launchtime
) b on b.wlmc_all = a.wuliaomc
  
left join(
SELECT wuliaomc,
min(riqi) 第一次出库时间,
max(riqi) 最近一次出库时间,
sum(case when company = '杭州游卡文化创意有限公司' then shifasl end) `文创业务销量`,
sum(case when company <> '杭州游卡文化创意有限公司' then shifasl end) `其他公司业务销量`,
sum(case when bumen = '渠道' then shifasl end) `渠道销量`,
sum(case when bumen = '电商平台部' then shifasl end) `电商销量`,
sum(case when riqi>date_sub(current_date(),interval 365 day) then shifasl end) `近一年销量`,
sum(shifasl) `总销量`,
sum(case when shifouzp = '是' then shifasl end) `赠品数量`,
sum(purchases) `总销售成本`,
sum(jiashuihj) `总销售额`,
sum(profit) `已售产品毛利`,
round(sum(jiashuihj)/sum(shifasl),2) 平均售价
FROM erp_jd_dwd.erp_jd_dwd_dim_saleshipping 
where kehumc not in ('杭州泳淳网络技术有限公司','杭州游卡文化创意有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司')
group by wuliaomc
) c on c.wuliaomc = a.wuliaomc

left join(
SELECT * FROM erp_jd_dwd.erp_jd_dwd_dim_cost
) d on d.wuliaomc = a.wuliaomc

left join(
SELECT wlmc_all,
count(distinct (danjubh_1)) 采购下单次数,
sum(caigousl_new) 采购数量,
sum(shengyurksl_new) 工厂结余,
sum(jiashuihj) 总投入
FROM erp_jd_dwd.erp_jd_dwd_dim_purchaseorders
group by wlmc_all
) e on e.wlmc_all = a.wuliaomc

left join(
SELECT wuliaomc,receiving 
FROM erp_jd_dwd.erp_jd_dwd_dim_beginninginventory
) f on f.wuliaomc = a.wuliaomc

left join(
SELECT wlmc_all,
sum(shifasl_new) 采购入库数量 
FROM erp_jd_dwd.erp_jd_dwd_dim_purchasereceiving 
group by wlmc_all
) g on g.wlmc_all = a.wuliaomc

left join(
SELECT wuliaomc,
sum(case when lingliaolxmc not in ('塑封','库存调整','调拨','返厂入库') and beizhu not like '%返厂%' and beizhu not like '%塑封%' then shifasl end) 公司领料 ,
sum(case when lingliaolxmc in ('塑封','库存调整','调拨','返厂入库') or beizhu like '%返厂%' or beizhu like '%塑封%' then shifasl end) 其他出库
FROM erp_jd_dwd.erp_jd_dwd_dim_othersshipping
group by wuliaomc
) h on h.wuliaomc = a.wuliaomc

left join(
SELECT wuliaomc,
sum(shishousl) 其他入库
FROM erp_jd_dwd.erp_jd_dwd_dim_othersreceiving
group by wuliaomc
) i on i.wuliaomc = a.wuliaomc

left join(
SELECT wuliaomc, 
sum(inventory) `库存结余`,
sum(case when cangkumc not like '%残次%' then inventory end) `库存结余（不含残次仓）`,
sum(case when cangkumc like '%电商%' or cangkumc like '%渠道%' or cangkumc like '%泳淳%' then inventory end) `库存结余（产品管理部统计）`
FROM erp_jd_dws.erp_jd_dws_warehouse 
group by wuliaomc
) j on j.wuliaomc = a.wuliaomc

left join(
SELECT wuliaomc,  
sum(shifasl) 退货 
FROM erp_jd_dwd.erp_jd_dwd_dim_salereturn 
where kehumc not in ('杭州泳淳网络技术有限公司','杭州游卡文化创意有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司')
group by wuliaomc
) k on k.wuliaomc = a.wuliaomc

left join(
SELECT 产品名称,  
sum(订单数量) 现货可发
FROM localdata.purchase_report 
where 生产状态 = '现货可发'
group by 产品名称
) l on l.产品名称 = a.wuliaomc

left join(
SELECT wuliaomc,
sum(case when `level` = '0-180天' then surplus end)  `0-180天`  ,
sum(case when `level` = '180-360天' then surplus end)  `180-360天`  ,
sum(case when `level` = '360天以上' then surplus end)  `360天以上`
FROM erp_jd_dws.erp_jd_dws_doi x
group by wuliaomc
) m on m.wuliaomc = a.wuliaomc

left join(
SELECT b.wlmc_all,
min(b.riqi) riqi,
a.首批采购数量
FROM erp_jd_dwd.erp_jd_dwd_dim_purchaseorders b
left join(
SELECT wlmc_all,
riqi,
sum(caigousl_new) 首批采购数量
FROM erp_jd_dwd.erp_jd_dwd_dim_purchaseorders
group by wlmc_all,riqi
) a on a.wlmc_all=b.wlmc_all and a.riqi = b.riqi
group by b.wlmc_all
) n on n.wlmc_all = a.wuliaomc

left join(
SELECT wuliaomc,
count(distinct(danjubh)) `客户下单次数`
FROM erp_jd_dwd.erp_jd_dwd_dim_saleorders
where danjulxmc = '标准销售订单'
and kehumc not in ('杭州泳淳网络技术有限公司','杭州游卡文化创意有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司')
group by wuliaomc
) o on o.wuliaomc = a.wuliaomc

left join(
SELECT wlmc_all,
sum(shituisl) 采购退料
FROM erp_jd_dwd.erp_jd_dwd_dim_purchasereturn
group by wuliaomc
) p on p.wlmc_all = a.wuliaomc

where a.wuliaomc not like '%-卡牌'
and a.wuliaomc not like '%-装配'
);