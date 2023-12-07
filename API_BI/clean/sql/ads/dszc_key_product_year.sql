drop table if exists www_bi_ads.dszc_key_product_year;
CREATE TABLE www_bi_ads.dszc_key_product_year( 
    SELECT 
    a.`classify` `产品大类`,
    a.`classify_1` `产品中类`,
    a.`classify_2` `产品小类`,
    a.`wuliaomc` `物料名称`,
    a.`year` `年`,
    b.riqi `上市日期`,
    d.cost `采购成本` ,
    e.`采购数量` ,
    e.`总投入` ,
    round(e.`采购数量`*c.平均售价,2)   `预计产出` ,
    f.receiving `期初库存` ,
    g.`采购入库数量` ,
    p.`采购退料` ,
    c.`销量` ,
    ifnull(f.receiving,0)+ifnull(g.`采购入库数量`,0)-ifnull(p.`采购退料`,0) `入库总量` ,
    round(c.`销量`/(ifnull(g.`采购入库数量`,0) + ifnull(f.receiving,0)),2) `动销率` ,
    c.`总销售成本` ,
    c.`总销售额` ,
    c.`平均售价` ,
    c.`已售产品毛利` ,
    round(ifnull(c.`总销售额`,0) - ifnull(e.`总投入`,0),2)   `总盈亏` ,
    round(c.`已售产品毛利`/c.`总销售额`,2)  `毛利率` ,
    ifnull(f.receiving,0)+ifnull(g.`采购入库数量`,0)-ifnull(p.`采购退料`,0)- ifnull(c.`销量`,0) `剩余产品价值` 
    FROM www_bi_ads.classify_year a

    left join(
        SELECT wlmc_all,
        year(riqi) `year`,
        riqi 
        FROM erp_jd_dws.erp_jd_dws_launchtime
    ) b on b.wlmc_all = a.wuliaomc AND b.`year` = a.`year`
      
    left join(
        SELECT wuliaomc,
        year(riqi) `year`,
        sum(shifasl) `销量`,
        sum(purchases) `总销售成本`,
        sum(jiashuihj) `总销售额`,
        sum(profit) `已售产品毛利`,
        round(sum(jiashuihj)/sum(shifasl),2) 平均售价
        FROM erp_jd_dws.erp_jd_dws_saleship_return
        where kehumc not in ('杭州泳淳网络技术有限公司','杭州游卡文化创意有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司','上海卡卡丫文化传播有限公司','杭州游卡文化创意有限公司拱墅区分公司')
        group by wuliaomc,year(riqi)
    ) c on c.wuliaomc = a.wuliaomc AND c.`year` = a.`year`

    left join(
        SELECT * FROM erp_jd_dwd.erp_jd_dwd_dim_cost
    ) d on d.wuliaomc = a.wuliaomc

    left join(
        SELECT wlmc_all,
        year(riqi) `year`,
        sum(caigousl_new) 采购数量,
        sum(jiashuihj) 总投入
        FROM erp_jd_dwd.erp_jd_dwd_dim_purchaseorders
        group by wlmc_all,year(riqi)
    ) e on e.wlmc_all = a.wuliaomc AND e.`year` = a.`year`

    left join(
        SELECT wuliaomc,
        year(riqi) `year`,
        receiving 
        FROM erp_jd_dwd.erp_jd_dwd_dim_beginninginventory
    ) f on f.wuliaomc = a.wuliaomc AND f.`year` = a.`year`

    left join(
        SELECT wlmc_all,
        year(riqi) `year`,
        sum(shifasl_new) 采购入库数量 
        FROM erp_jd_dwd.erp_jd_dwd_dim_purchasereceiving 
        group by wlmc_all,year(riqi)
    ) g on g.wlmc_all = a.wuliaomc AND g.`year` = a.`year`

    left join(
        SELECT wlmc_all,
        year(tuiliaorq) `year`,
        sum(shituisl) 采购退料
        FROM erp_jd_dwd.erp_jd_dwd_dim_purchasereturn
        group by wuliaomc,year(tuiliaorq)
    ) p on p.wlmc_all = a.wuliaomc AND p.`year` = a.`year`
      
    WHERE a.`classify_1` like '%电商%'
    and c.`销量`<>0

);